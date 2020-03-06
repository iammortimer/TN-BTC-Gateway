import os
import sqlite3 as sqlite
import time
import PyCWaves
import traceback
import sharedfunc
import bitcoinrpc.authproxy as authproxy

class OtherChecker(object):
    def __init__(self, config):
        self.config = config
        self.dbCon = sqlite.connect('gateway.db')
        self.myProxy = self.getProxy()

        self.pwTN = PyCWaves.PyCWaves()
        self.pwTN.setNode(node=self.config['tn']['node'], chain=self.config['tn']['network'], chain_id='L')
        seed = os.getenv(self.config['tn']['seedenvname'], self.config['tn']['gatewaySeed'])
        #self.tnAddress = self.pwTN.Address(seed=seed)
        self.tnAsset = self.pwTN.Asset(self.config['tn']['assetId'])

        cursor = self.dbCon.cursor()
        self.lastScannedBlock = cursor.execute('SELECT height FROM heights WHERE chain = "Other"').fetchall()[0][0]

    def getProxy(self):
        instance = None

        if self.config['other']['node'].startswith('http'):
            instance = authproxy.AuthServiceProxy(self.config['other']['node'])
        else:
            instance = authproxy.AuthServiceProxy()

        return instance

    def getCurrentBlock(self):
        latestBlock = self.myProxy.getblock(self.myProxy.getbestblockhash())

        return latestBlock['height']

    def getBlock(self, height):
        blockhash = self.myProxy.getblockhash(height)
        block = self.myProxy.getblock(blockhash)

        return block

    def run(self):
        #main routine to run continuesly
        print('started checking Other blocks at: ' + str(self.lastScannedBlock))

        self.dbCon = sqlite.connect('gateway.db')
        while True:
            try:
                nextblock = self.getCurrentBlock() - self.config['other']['confirmations']

                if nextblock > self.lastScannedBlock:
                    self.lastScannedBlock += 1
                    self.checkBlock(self.lastScannedBlock)
                    cursor = self.dbCon.cursor()
                    cursor.execute('UPDATE heights SET "height" = ' + str(self.lastScannedBlock) + ' WHERE "chain" = "Other"')
                    self.dbCon.commit()
            except Exception as e:
                self.lastScannedBlock -= 1
                print('Something went wrong during Other block iteration: ')
                print(traceback.TracebackException.from_exception(e))

            time.sleep(self.config['other']['timeInBetweenChecks'])

    def checkBlock(self, heightToCheck):
        #check content of the block for valid transactions
        block = self.getBlock(heightToCheck)
        for transaction in block['tx']:
            txInfo = self.checkTx(transaction)

            if txInfo is not None:
                cursor = self.dbCon.cursor()
                res = cursor.execute('SELECT targetAddress FROM tunnel WHERE sourceAddress ="' + txInfo['sender'] + '"').fetchall()
                if len(res) == 0:
                    self.faultHandler(txInfo, 'notunnel')
                else:
                    targetAddress = res[0][0]
                    amount = txInfo['amount']
                    amount -= self.config['tn']['fee']
                    amount *= pow(10, self.config['tn']['decimals'])
                    amount = int(round(amount))

                    try:
                        addr = self.pwTN.Address(targetAddress)
                        if self.config['tn']['assetId'] == 'TN':
                            tx = self.tnAddress.sendWaves(addr, amount, 'Thanks for using our service!', txFee=2000000)
                        else:
                            tx = self.tnAddress.sendAsset(addr, self.tnAsset, amount, 'Thanks for using our service!', txFee=2000000)

                        if 'error' in tx:
                            self.faultHandler(txInfo, "senderror", e=tx['message'])
                        else:
                            print("send tx: " + str(tx))

                            cursor = self.dbCon.cursor()
                            amount /= pow(10, self.config['tn']['decimals'])
                            cursor.execute('INSERT INTO executed ("sourceAddress", "targetAddress", "ethTxId", "tnTxId", "amount", "amountFee") VALUES ("' + txInfo['sender'] + '", "' + targetAddress + '", "' + transaction.hex() + '", "' + tx['id'] + '", "' + str(round(amount)) + '", "' + str(self.config['tn']['fee']) + '")')
                            self.dbCon.commit()
                            print('send tokens from waves to tn!')

                            cursor = self.dbCon.cursor()
                            cursor.execute('DELETE FROM tunnel WHERE sourceAddress = "' + txInfo['sender'] + '" and targetAddress = "' + targetAddress + '"')
                            self.dbCon.commit()
                            
                    except Exception as e:
                        self.faultHandler(txInfo, "txerror", e=e)

    def checkTx(self, tx):
        #check the transaction
        result = None
        transaction = self.myProxy.getrawtransaction(tx,True)
        receivers = self.getReceivers(transaction)

        for receiver in receivers:
            if receiver['address'] == self.config['other']['gatewayAddress']:
                senders = self.getSenders(transaction)
                if len(senders) == 1:
                    sender = senders[0]
                
                    recipient = receiver['address']
                    amount = receiver['amount'] / 10 ** self.config['other']['decimals']

                    cursor = self.dbCon.cursor()
                    res = cursor.execute('SELECT tnTxId FROM executed WHERE otherTxId = "' + transaction['txid'] + '"').fetchall()
                    if len(res) == 0: result =  { 'sender': sender, 'function': 'transfer', 'recipient': recipient, 'amount': amount, 'id': transaction['txid'] }
                else:
                    print(sharedfunc.getnow() + "TODO: Handle multiple senders, txid: " + transaction['txid'])

        return result
        
    def getReceivers(self, tx):
        results = list()

        receiver = {}

        for vout in tx['vout']:
            if 'addresses' not in vout['scriptPubKey']:
                continue
        
            for address in vout['scriptPubKey']['addresses']:
                receiver['address'] = address
                receiver['amount'] = vout['value']

                results.append(receiver)

        return results

    def getSenders(self, tx):
        results = list() 

        if 'vin' not in tx:
            return results

        for vin in tx['vin']:
            if ('txid' not in vin) or ('vout' not in vin):
                continue

            vin_transaction = self.myProxy.getrawtransaction(vin['txid'],True)

            if 'addresses' not in vin_transaction['vout'][vin['vout']]['scriptPubKey']:
                continue

            for address in vin_transaction['vout'][vin['vout']]['scriptPubKey']['addresses']:
                if address not in results:
                    results.append(address)

        return results


    def faultHandler(self, tx, error, e="", senders=object):
        #handle transfers to the gateway that have problems
        amount = tx['amount']
        timestampStr = sharedfunc.getnow()

        if error == "notunnel":
            cursor = self.dbCon.cursor()
            cursor.execute('INSERT INTO errors ("sourceAddress", "targetAddress", "tnTxId", "otherTxId", "amount", "error") VALUES ("' + tx['sender'] + '", "", "", "' + tx['id'] + '", "' + str(amount) + '", "no tunnel found for sender")')
            self.dbCon.commit()
            print(timestampStr + " - Error: no tunnel found for transaction from " + tx['sender'] + " - check errors table.")

        if error == "txerror":
            targetAddress = tx['recipient']
            cursor = self.dbCon.cursor()
            cursor.execute('INSERT INTO errors ("sourceAddress", "targetAddress", "tnTxId", "otherTxId", "amount", "error", "exception") VALUES ("' + tx['sender'] + '", "' + targetAddress + '", "", "' + tx['id'] + '", "' + str(amount) + '", "tx error, possible incorrect address", "' + str(e) + '")')
            self.dbCon.commit()
            print(timestampStr + " - Error: on outgoing transaction for transaction from " + tx['sender'] + " - check errors table.")

        if error == "senderror":
            targetAddress = tx['recipient']
            cursor = self.dbCon.cursor()
            cursor.execute('INSERT INTO errors ("sourceAddress", "targetAddress", "tnTxId", "otherTxId", "amount", "error", "exception") VALUES ("' + tx['sender'] + '", "' + targetAddress + '", "", "' + tx['id'] + '", "' + str(amount) + '", "tx error, check exception error", "' + str(e) + '")')
            self.dbCon.commit()
            print(timestampStr + " - Error: on outgoing transaction for transaction from " + tx['sender'] + " - check errors table.")
