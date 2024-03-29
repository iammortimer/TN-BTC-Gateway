import sqlite3 as sqlite
from datetime import timedelta
import datetime
import os

class dbCalls(object):
    def __init__(self, config):
        self.config = config

        if self.config["main"]["db-location"] != "":
            path= os.getcwd()
            dbfile = path + '/' + self.config["main"]["db-location"] + '/' + 'gateway.db'
            dbfile = os.path.normpath(dbfile)
        else:
            dbfile = 'gateway.db'

        self.dbCon = sqlite.connect(dbfile, check_same_thread=False)

#DB Setup part
    def createdb(self):
        createHeightTable = '''
            CREATE TABLE IF NOT EXISTS heights (
                id integer PRIMARY KEY,
                chain text NOT NULL,
                height integer
            );
        '''
        createTunnelTable = '''
            CREATE TABLE IF NOT EXISTS tunnel (
                id integer PRIMARY KEY,
                sourceAddress text NOT NULL,
                targetAddress text NOT NULL,
                timestamp timestamp
                default current_timestamp,
                status text
            );
        '''
        createTableExecuted = '''
            CREATE TABLE IF NOT EXISTS executed (
                id integer PRIMARY KEY,
                sourceAddress text NOT NULL,
                targetAddress text NOT NULL,
                tnTxId text NOT NULL,
                otherTxId text NOT NULL,
                timestamp timestamp
                default current_timestamp,
                amount real,
                amountFee real
        );
        '''
        createTableErrors = '''
            CREATE TABLE IF NOT EXISTS errors (
                id integer PRIMARY KEY,
                sourceAddress text ,
                targetAddress text ,
                tnTxId text ,
                otherTxId text ,
                timestamp timestamp
                default current_timestamp,
                amount real,
                error text,
                exception text
        );
        '''
        cursor = self.dbCon.cursor()
        cursor.execute(createHeightTable)
        cursor.execute(createTunnelTable)
        cursor.execute(createTableExecuted)
        cursor.execute(createTableErrors)
        self.dbCon.commit()

    def createVerify(self):
        createVerifyTable = '''
            CREATE TABLE IF NOT EXISTS verified (
                id integer PRIMARY KEY,
                chain text NOT NULL,
                tx text NOT NULL,
                block integer
            );
        '''
        cursor = self.dbCon.cursor()
        cursor.execute(createVerifyTable)
        self.dbCon.commit()

    def updateExisting(self):
        try:
            sql = 'ALTER TABLE tunnel ADD COLUMN timestamp timestamp;'

            cursor = self.dbCon.cursor()
            cursor.execute(sql)
            self.dbCon.commit()

            sql = 'ALTER TABLE tunnel ADD COLUMN status text;'

            cursor = self.dbCon.cursor()
            cursor.execute(sql)
            self.dbCon.commit()

            sql = 'UPDATE tunnel SET status = "created"'

            cursor = self.dbCon.cursor()
            cursor.execute(sql)
            self.dbCon.commit()
        except:
            return

#heights table related
    def lastScannedBlock(self, chain):
        sql = 'SELECT height FROM heights WHERE chain = ?'
        values = (chain,)

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql, values).fetchall()
        cursor.close()

        if len(qryResult) > 0:
            return qryResult[0][0]
        else:
            return 0

    def getHeights(self):
        sql = 'SELECT chain, height FROM heights'

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql).fetchall()
        cursor.close()

        if len(qryResult) > 0:
            return qryResult
        else:
            return {}

    def updHeights(self, block, chain):
        sql = 'UPDATE heights SET "height" = ? WHERE chain = ?'
        values = (block, chain)

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql, values)
        self.dbCon.commit()
        cursor.close()

    def insHeights(self, block, chain):
        sql = 'INSERT INTO heights ("chain", "height") VALUES (?, ?)'
        values = (chain, block)

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql, values)
        self.dbCon.commit()
        cursor.close()

#tunnel table related
    def doWeHaveTunnels(self):
        sql = 'SELECT * FROM tunnel WHERE status = "created"'

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql).fetchall()
        cursor.close()

        if len(qryResult) > 0:
            return True
        else:
            return False

    def getTargetAddress(self, sourceAddress):
        sql = 'SELECT targetAddress FROM tunnel WHERE status <> "error" AND sourceAddress = ?'
        values = (sourceAddress,)

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql, values).fetchall()
        cursor.close()

        if len(qryResult) > 0:
            return qryResult[0][0]
        else:
            return {}

    def getSourceAddress(self, targetAddress):
        if targetAddress == '':
            sql = 'SELECT sourceAddress FROM tunnel WHERE status = "created"'

            cursor = self.dbCon.cursor()
            qryResult = cursor.execute(sql).fetchall()
            cursor.close()
        else:
            sql = 'SELECT sourceAddress FROM tunnel WHERE status <> "error" AND targetAddress = ?'
            values = (targetAddress,)

            cursor = self.dbCon.cursor()
            qryResult = cursor.execute(sql, values).fetchall()
            cursor.close()

        if len(qryResult) > 0:
            return qryResult
        else:
            return {}

    def getTunnelStatus(self, targetAddress = '', sourceAddress = ''):
        if targetAddress != '':
            sql = 'SELECT status FROM tunnel WHERE targetAddress = ? ORDER BY id DESC LIMIT 1'
            values = (targetAddress,)
        elif  sourceAddress != '':
            sql = 'SELECT status FROM tunnel WHERE sourceAddress = ? ORDER BY id DESC LIMIT 1'
            values = (sourceAddress,)
        else:
            return {}

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql, values).fetchall()
        cursor.close()

        if len(qryResult) > 0:
            return qryResult
        else:
            return {}

    def getTunnels(self, status = ''):
        if status != '':
            sql = 'SELECT sourceAddress, targetAddress FROM tunnel WHERE status = ?'
            values = (status,)
        else:
            return {}

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql, values).fetchall()
        cursor.close()

        if len(qryResult) > 0:
            return qryResult
        else:
            return {}

    def insTunnel(self, status, sourceAddress, targetAddress):
        sql = 'INSERT INTO tunnel ("sourceAddress", "targetAddress", "status", "timestamp") VALUES (?, ?, ?, CURRENT_TIMESTAMP)'
        values = (sourceAddress, targetAddress, status)

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql, values)
        self.dbCon.commit()
        cursor.close()

    def updTunnel(self, status, sourceAddress, targetAddress, statusOld = ''):
        if statusOld == '':
            statusOld = 'created'

        sql = 'UPDATE tunnel SET "status" = ?, "timestamp" = CURRENT_TIMESTAMP WHERE status = ? AND sourceAddress = ? and targetAddress = ?'
        values = (status, statusOld, sourceAddress, targetAddress)

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql, values)
        self.dbCon.commit()
        cursor.close()

    def delTunnel(self, sourceAddress, targetAddress):
        sql = 'DELETE FROM tunnel WHERE sourceAddress = ? and targetAddress = ?'
        values = (sourceAddress, targetAddress)

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql, values)
        self.dbCon.commit()
        cursor.close()

#executed table related
    def insExecuted(self, sourceAddress, targetAddress, otherTxId, tnTxID, amount, amountFee):
        sql = 'INSERT INTO executed ("sourceAddress", "targetAddress", "otherTxId", "tnTxId", "amount", "amountFee") VALUES (?, ?, ?, ?, ?, ?)'
        values = (sourceAddress, targetAddress, otherTxId, tnTxID, amount, amountFee)

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql, values)
        self.dbCon.commit()
        cursor.close()

    def updExecuted(self, id, sourceAddress, targetAddress, otherTxId, tnTxID, amount, amountFee):
        sql = 'UPDATE executed SET "sourceAddress" = ?, "targetAddress" = ?, "otherTxId" = ?, "tnTxId" = ?, "amount" = ?, "amountFee" = ?) WHERE id = ?'
        values = (sourceAddress, targetAddress, otherTxId, tnTxID, amount, amountFee, id)

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql, values)
        self.dbCon.commit()
        cursor.close()

    def didWeSendTx(self, txid):
        sql = 'SELECT * FROM executed WHERE (otherTxId = ? OR tnTxId = ?)'
        values = (txid, txid)

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql, values).fetchall()
        cursor.close()

        if len(qryResult) > 0:
            return True
        else:
            return False

    def getExecutedAll(self):
        sql = 'SELECT * FROM executed'

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql).fetchall()
        cursor.close()

        if len(qryResult) > 0:
            return qryResult
        else:
            return {}

    def getExecuted(self, sourceAddress = '', targetAddress = '', otherTxId = '', tnTxId = ''):
        if sourceAddress != '':
            sql = 'SELECT otherTxId FROM executed WHERE sourceAddress = ? ORDER BY id DESC LIMIT 1'
            values = (sourceAddress,)
        elif targetAddress != '':
            sql = 'SELECT tnTxId FROM executed WHERE targetAddress = ? ORDER BY id DESC LIMIT 1'
            values = (targetAddress,)
        elif otherTxId != '':
            sql = 'SELECT * FROM executed WHERE otherTxId = ? ORDER BY id DESC LIMIT 1'
            values = (otherTxId,)
        elif tnTxId != '':
            sql = 'SELECT * FROM executed WHERE tnTxId = ? ORDER BY id DESC LIMIT 1'
            values = (tnTxId,)
        else:
            return {}

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql, values).fetchall()
        cursor.close()

        if len(qryResult) > 0:
            return qryResult
        else:
            return {}

#error table related
    def insError(self, sourceAddress, targetAddress, tnTxId, otherTxId, amount, error, exception = ''):
        sql = 'INSERT INTO errors ("sourceAddress", "targetAddress", "tnTxId", "otherTxId", "amount", "error", "exception") VALUES (?, ?, ?, ?, ?, ?, ?)'
        values = (sourceAddress, targetAddress, tnTxId, otherTxId, amount, error, exception)

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql, values)
        self.dbCon.commit()
        cursor.close()

    def getErrors(self):
        sql = 'SELECT * FROM errors'

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql).fetchall()
        cursor.close()

        if len(qryResult) > 0:
            return qryResult
        else:
            return {}

    def getError(self, sourceAddress='', targetAddress=''):
        if sourceAddress != '':
            sql = 'SELECT error, tntxid, otherTxId FROM errors WHERE sourceAddress = ? ORDER BY id DESC LIMIT 1'
            values = (sourceAddress,)
        elif targetAddress != '':
            sql = 'SELECT error, tntxid, otherTxId FROM errors WHERE targetAddress = ? ORDER BY id DESC LIMIT 1'
            values = (targetAddress,)
        else:
            return {}

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql, values).fetchall()
        cursor.close()

        if len(qryResult) > 0:
            return qryResult
        else:
            return {}

#verified table related
    def getVerifiedAll(self):
        sql = 'SELECT * FROM verified'

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql).fetchall()
        cursor.close()

        if len(qryResult) > 0:
            return qryResult
        else:
            return {}

    def getUnVerified(self):
        sql = 'SELECT * FROM verified WHERE block = 0'

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql).fetchall()
        cursor.close()

        if len(qryResult) > 0:
            return qryResult
        else:
            return {}

    def getVerified(self, tx):
        sql = 'SELECT block FROM verified WHERE tx = ?'
        values = (tx,)

        cursor = self.dbCon.cursor()
        qryResult = cursor.execute(sql, values).fetchall()
        cursor.close()

        if len(qryResult) > 0:
            return qryResult[0][0]
        else:
            return None

    def insVerified(self, chain, tx, block):
        if self.getVerified(tx) is None:
            sql = 'INSERT INTO verified ("chain", "tx", "block") VALUES (?, ?, ?)'
            values = (chain, tx, block)

            cursor = self.dbCon.cursor()
            qryResult = cursor.execute(sql, values)
            self.dbCon.commit()
            cursor.close()
        else:
            sql = 'UPDATE verified SET "block" = ? WHERE tx = ?'
            values = (block, tx)

            cursor = self.dbCon.cursor()
            qryResult = cursor.execute(sql, values)
            self.dbCon.commit()
            cursor.close()

#other
    def checkTXs(self, address):
        if address == '':
            cursor = self.dbCon.cursor()
            sql = "SELECT e.sourceAddress, e.targetAddress, e.tnTxId, e.otherTxId as 'OtherTxId', ifnull(v.block, 0) as 'TNVerBlock', ifnull(v2.block, 0) as 'OtherVerBlock', e.amount, CASE WHEN e.targetAddress LIKE '3J%' THEN 'Deposit' ELSE 'Withdraw' END 'TypeTX', " \
            "CASE WHEN e.targetAddress LIKE '3J%' AND v.block IS NOT NULL THEN 'verified' WHEN e.targetAddress NOT LIKE '3J%' AND v2.block IS NOT NULL AND v2.block IS NOT 0 THEN 'verified' ELSE 'unverified' END 'Status' " \
            "FROM executed e LEFT JOIN verified v ON e.tnTxId = v.tx LEFT JOIN verified v2 ON e.otherTxId = v2.tx "
            cursor.execute(sql)
        else:
            cursor = self.dbCon.cursor()
            sql = "SELECT e.sourceAddress, e.targetAddress, e.tnTxId, e.otherTxId as 'OtherTxId', ifnull(v.block, 0) as 'TNVerBlock', ifnull(v2.block, 0) as 'OtherVerBlock', e.amount, CASE WHEN e.targetAddress LIKE '3J%' THEN 'Deposit' ELSE 'Withdraw' END 'TypeTX', " \
            "CASE WHEN e.targetAddress LIKE '3J%' AND v.block IS NOT NULL THEN 'verified' WHEN e.targetAddress NOT LIKE '3J%' AND v2.block IS NOT NULL AND v2.block IS NOT 0 THEN 'verified' ELSE 'unverified' END 'Status' " \
            "FROM executed e LEFT JOIN verified v ON e.tnTxId = v.tx LEFT JOIN verified v2 ON e.otherTxId = v2.tx WHERE (e.sourceAddress = ? or e.targetAddress = ?)"
            cursor.execute(sql, (address, address))

        tx = [dict((cursor.description[i][0], value) for i, value in enumerate(row)) for row in cursor.fetchall()]
        cursor.close()

        if len(tx) == 0:
            return {'error': 'no tx found'}
        else:
            return tx

    def getFees(self, fromdate, todate):
        #check date notation
        if len(fromdate) != 0:
            fromyear,frommonth,fromday = fromdate.split('-')

            isValidFromDate = True
            try :
                datetime.datetime(int(fromyear),int(frommonth),int(fromday))
            except ValueError :
                isValidFromDate = False
        else:
            isValidFromDate = False

        if len(todate) != 0:
            toyear,tomonth,today = todate.split('-')

            isValidtoDate = True
            try :
                datetime.datetime(int(toyear),int(tomonth),int(today))
            except ValueError :
                isValidtoDate = False
        else:
            isValidtoDate = False

        if not isValidFromDate:
            fromdate = '1990-01-01'
    
        if not isValidtoDate:
            todat = datetime.date.today() + timedelta(days=1)
            todate = todat.strftime('%Y-%m-%d')
        
        values = (fromdate, todate)

        sql = "SELECT SUM(amountFee) as totalFee from executed WHERE timestamp > ? and timestamp < ?"
        cursor = self.dbCon.cursor()
        result = cursor.execute(sql, values).fetchall()
        cursor.close()

        if len(result) == 0:
            Fees = 0
        else:
            Fees = result[0][0]

        return { 'totalFees': Fees }
