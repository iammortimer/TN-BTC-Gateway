import os
import sqlite3 as sqlite
import time
import PyCWaves
import traceback
import sharedfunc
import bitcoinrpc.authproxy as authproxy
from verification import verifier

class OtherChecker(object):
    def __init__(self, config):
        self.config = config
        self.dbCon = sqlite.connect('gateway.db')
        self.myProxy = authproxy.AuthServiceProxy(self.config['other']['node'])

        self.pwTN = PyCWaves.PyCWaves()
        self.pwTN.setNode(node=self.config['tn']['node'], chain=self.config['tn']['network'], chain_id='L')
        self.pwTN.THROW_EXCEPTION_ON_ERROR = True
        seed = os.getenv(self.config['tn']['seedenvname'], self.config['tn']['gatewaySeed'])
        self.tnAddress = self.pwTN.Address(seed=seed)
        self.tnAsset = self.pwTN.Asset(self.config['tn']['assetId'])
        self.verifier = verifier(config)

        cursor = self.dbCon.cursor()
        self.lastScannedBlock = cursor.execute('SELECT height FROM heights WHERE chain = "Other"').fetchall()[0][0]

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
                            cursor.execute('INSERT INTO executed ("sourceAddress", "targetAddress", "otherTxId", "tnTxId", "amount", "amountFee") VALUES ("' + txInfo['sender'] + '", "' + targetAddress + '", "' + txInfo['id'] + '", "' + tx['id'] + '", "' + str(round(amount)) + '", "' + str(self.config['tn']['fee']) + '")')
                            self.dbCon.commit()
                            print(self.config['main']['name'] + ' tokens deposited on tn!')

                            cursor = self.dbCon.cursor()
                            cursor.execute('DELETE FROM tunnel WHERE sourceAddress = "' + txInfo['sender'] + '" and targetAddress = "' + targetAddress + '"')
                            self.dbCon.commit()
                            
                    except Exception as e:
                        self.faultHandler(txInfo, "txerror", e=e)

                    self.verifier.verifyTN(tx)

    def checkTx(self, tx):
        #check the transaction
        result = None
        transaction = self.myProxy.getrawtransaction(tx,True)
        receivers = self.getReceivers(transaction)
        cursor = self.dbCon.cursor()
        tunnels = cursor.execute('SELECT sourceAddress FROM tunnel').fetchall()

        for receiver in receivers:
            for tunnel in tunnels:
                if receiver['address'] == tunnel[0]:
                    sender = receiver['address']
                    amount = receiver['amount']

                    res = cursor.execute('SELECT tnTxId FROM executed WHERE otherTxId = "' + transaction['txid'] + '"').fetchall()
                    if len(res) == 0: result =  { 'sender': sender, 'function': 'transfer', 'amount': amount, 'id': transaction['txid'] }

        return result
        
    def getReceivers(self, tx):
        results = list()

        for vout in tx['vout']:
            if 'addresses' not in vout['scriptPubKey']:
                continue
        
            for address in vout['scriptPubKey']['addresses']:
                receiver = {}

                receiver['address'] = address
                receiver['amount'] = vout['value']

                results.append(receiver)

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
