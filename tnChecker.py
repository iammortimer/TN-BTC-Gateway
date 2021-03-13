import time
import traceback
import base58
import sharedfunc
from dbClass import dbCalls
from dbPGClass import dbPGCalls
from tnClass import tnCalls
from otherClass import otherCalls
from verification import verifier

class TNChecker(object):
    def __init__(self, config, db = None):
        self.config = config

        if db == None:
            if self.config['main']['use-pg']:
                self.db = dbPGCalls(config)
            else:
                self.db = dbCalls(config)
        else:
            self.db = db

        self.tnc = tnCalls(config, self.db)
        self.verifier = verifier(config, self.db)

        self.lastScannedBlock = self.db.lastScannedBlock("TN")

    def run(self):
        #main routine to run continuesly
        #print('INFO: started checking tn blocks at: ' + str(self.lastScannedBlock))

        while True:
            try:
                nextblock = self.tnc.currentBlock() - self.config['tn']['confirmations']

                if nextblock > self.lastScannedBlock:
                    self.lastScannedBlock += 1
                    self.checkBlock(self.lastScannedBlock)
                    self.db.updHeights(self.lastScannedBlock, 'TN')
            except Exception as e:
                self.lastScannedBlock -= 1
                print('ERROR: Something went wrong during tn block iteration: ' + str(traceback.TracebackException.from_exception(e)))

            time.sleep(self.config['tn']['timeInBetweenChecks'])

    def checkBlock(self, heightToCheck):
        #check content of the block for valid transactions
        block = self.tnc.getBlock(heightToCheck)
        for transaction in block['transactions']:
            targetAddress = self.tnc.checkTx(transaction)

            if targetAddress is not None:
                if targetAddress != "No attachment":
                    if not(otherCalls(self.config, self.db).validateaddress(targetAddress)):
                        self.faultHandler(transaction, "txerror")
                    else:
                        targetAddress = otherCalls(self.config, self.db).normalizeAddress(targetAddress)
                        amount = transaction['amount'] / pow(10, self.config['tn']['decimals'])
                        amount = round(amount, 8)
                        
                        if amount < self.config['main']['min'] or amount > self.config['main']['max']:
                            self.faultHandler(transaction, "senderror", e='outside amount ranges')
                        else:
                            try:
                                txId = None
                                self.db.insTunnel('sending', transaction['sender'], targetAddress)
                                txId = otherCalls(self.config, self.db).sendTx(targetAddress, amount)

                                if 'error' in txId:
                                    self.faultHandler(transaction, "senderror", e=txId)
                                    self.db.updTunnel("error", transaction['sender'], targetAddress, statusOld="sending")
                                else:
                                    print("INFO: send tx: " + str(txId))

                                    self.db.insExecuted(transaction['sender'], targetAddress, txId, transaction['id'], amount, self.config['other']['fee'])
                                    print('INFO: send tokens from tn to other!')

                                    #self.db.delTunnel(transaction['sender'], targetAddress)
                                    self.db.updTunnel("verifying", transaction['sender'], targetAddress, statusOld='sending')
                            except Exception as e:
                                self.faultHandler(transaction, "txerror", e=e)
                                continue

                            if txId is None:
                                if targetAddress != 'invalid address':
                                    self.db.insError(transaction['sender'], targetAddress, transaction['id'], '', amount, 'tx failed to send - manual intervention required')
                                    print("ERROR: tx failed to send - manual intervention required")
                                    self.db.updTunnel("error", transaction['sender'], targetAddress, statusOld="sending")
                            else:
                                otherCalls(self.config, self.db).verifyTx(txId, transaction['sender'], targetAddress)
                else:
                    self.faultHandler(transaction, 'noattachment')
        
    def faultHandler(self, tx, error, e=""):
        #handle transfers to the gateway that have problems
        amount = tx['amount'] / pow(10, self.config['tn']['decimals'])
        timestampStr = sharedfunc.getnow()

        if error == "noattachment":
            self.db.insError(tx['sender'], "", tx['id'], "", amount, "no attachment found on transaction")
            print("ERROR: " + timestampStr + " - Error: no attachment found on transaction from " + tx['sender'] + " - check errors table.")

        if error == "txerror":
            targetAddress = base58.b58decode(tx['attachment']).decode()
            self.db.insError(tx['sender'], targetAddress, tx['id'], "", amount, "tx error, possible incorrect address", str(e))
            print("ERROR: " + timestampStr + " - Error: on outgoing transaction for transaction from " + tx['sender'] + " - check errors table.")

        if error == "senderror":
            targetAddress = base58.b58decode(tx['attachment']).decode()
            self.db.insError(tx['sender'], targetAddress, tx['id'], "", amount, "tx error, check exception error", str(e))
            print("ERROR: " + timestampStr + " - Error: on outgoing transaction for transaction from " + tx['sender'] + " - check errors table.")
