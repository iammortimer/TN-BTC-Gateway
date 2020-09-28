import os
import traceback
import bitcoinrpc.authproxy as authproxy
from dbClass import dbCalls
from dbPGClass import dbPGCalls

class otherCalls(object):
    def __init__(self, config):
        self.config = config

        if self.config['main']['use-pg']:
            self.db = dbPGCalls(config)
        else:
            self.db = dbCalls(config)

        self.myProxy = authproxy.AuthServiceProxy(self.config['other']['node'])

        self.lastScannedBlock = self.db.lastScannedBlock("Other")

    def currentBlock(self):
        result = self.myProxy.getblock(self.myProxy.getbestblockhash())

        return result['height']

    def getBlock(self, height):
        blockhash = self.myProxy.getblockhash(height)
        block = self.myProxy.getblock(blockhash)

        return block

    def currentBalance(self):
        balance = self.myProxy.getbalance()

        return balance

    def normalizeAddress(self, address):
        if self.validateAddress(address):
            return address
        else:
            return "invalid address"

    def validateAddress(self, address):
        return self.myProxy.validateAddress(address)['isvalid']

    def getNewAddress(self):
        return self.myProxy.getnewaddress()

    def verifyTx(self, txId, sourceAddress = '', targetAddress = ''):
        tx = self.db.getExecuted(otherTxId=txId)

        try:
            verified = self.myProxy.gettransaction(txId)
            block = self.myProxy.getblock(verified['blockhash'])

            if verified['status'] == 1:
                self.db.insVerified("Other", txId, block)
                print('INFO: tx to other verified!')

                self.db.delTunnel(sourceAddress, targetAddress)
            elif verified['status'] == 0:
                print('ERROR: tx failed to send!')
                self.resendTx(txId)
        except:
            self.db.insVerified("Other", txId, 0)
            print('WARN: tx to other not verified!')
  
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

    def checkTx(self, tx):
        #check the transaction
        result = None
        transaction = self.myProxy.getrawtransaction(tx,True)
        receivers = self.getReceivers(transaction)
        tunnels = self.db.getSourceAddress('')

        for receiver in receivers:
            for tunnel in tunnels:
                if receiver['address'] == tunnel:
                    sender = receiver['address']
                    amount = receiver['amount']

                    if not self.db.didWeSendTx(tx.hex()): 
                        result = { 'sender': sender, 'function': 'transfer', 'recipient': '', 'amount': amount, 'id': transaction['txid'] }

        return result

    def sendTx(self, targetAddress, amount):
        amount -= self.config['other']['fee']

        passphrase = os.getenv(self.config['other']['passenvname'], self.config['other']['passphrase'])

        if len(passphrase) > 0:
            self.myProxy.walletpassphrase(passphrase, 30)

        txId = self.myProxy.sendtoaddress(targetAddress, amount)

        if len(passphrase) > 0:
            self.myProxy.walletlock()

        return txId

    def resendTx(self, txId):
        if type(txId) == str:
            txid = txId
        else: 
            txid = txId.hex()

        failedtx = self.db.getExecuted(otherTxId=txid)

        if len(failedtx) > 0:
            id = failedtx[0][0]
            sourceAddress = failedtx[0][1]
            targetAddress = failedtx[0][2]
            tnTxId = failedtx[0][3]
            amount = failedtx[0][6]

            self.db.insError(sourceAddress, targetAddress, tnTxId, txid, amount, 'tx failed on network - manual intervention required')
            print("ERROR: tx failed on network - manual intervention required: " + txid)
            self.db.updTunnel("error", sourceAddress, targetAddress, statusOld="verifying")

