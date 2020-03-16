import re
import sqlite3 as sqlite
import bitcoinrpc.authproxy as authproxy
import os
import PyCWaves
import json
from verification import verifier

from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.status import HTTP_401_UNAUTHORIZED
import secrets
import uvicorn
from starlette.requests import Request
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from starlette.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBasic()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

with open('config.json') as json_file:
    config = json.load(json_file)

def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, config["main"]["admin-username"])
    correct_password = secrets.compare_digest(credentials.password, config["main"]["admin-password"])
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

def get_tnBalance():
    pwTN = PyCWaves.PyCWaves()
    pwTN.THROW_EXCEPTION_ON_ERROR = True
    pwTN.setNode(node=config['tn']['node'], chain=config['tn']['network'], chain_id='L')
    seed = os.getenv(config['tn']['seedenvname'], config['tn']['gatewaySeed'])
    tnAddress = pwTN.Address(seed=seed)
    myBalance = tnAddress.balance(assetId=config['tn']['assetId'])
    myBalance /= pow(10, config['tn']['decimals'])
    return int(round(myBalance))

def get_otherBalance():
    otherProxy = authproxy.AuthServiceProxy(config['other']['node'])
    balance = otherProxy.getbalance()
    return int(round(balance))


@app.get("/")
async def index(request: Request):
    heights = await getHeights()
    return templates.TemplateResponse("index.html", {"request": request, 
                                                     "chainName": config['main']['name'],
                                                     "assetID": config['tn']['assetId'],
                                                     "tn_gateway_fee":config['tn']['gateway_fee'],
                                                     "tn_network_fee":config['tn']['network_fee'],
                                                     "tn_total_fee":config['tn']['network_fee']+config['tn']['gateway_fee'],
                                                     "eth_gateway_fee":config['other']['gateway_fee'],
                                                     "eth_network_fee":config['other']['network_fee'],
                                                     "eth_total_fee":config['other']['network_fee'] + config['other']['gateway_fee'],
                                                     "fee": config['tn']['fee'],
                                                     "company": config['main']['company'],
                                                     "email": config['main']['contact-email'],
                                                     "telegram": config['main']['contact-telegram'],
                                                     "recovery_amount":config['main']['recovery_amount'],
                                                     "recovery_fee":config['main']['recovery_fee'],
                                                     "ethHeight": heights['Other'],
                                                     "tnHeight": heights['TN'],
                                                     "tnAddress": config['tn']['gatewayAddress'],
                                                     "ethAddress": config['other']['gatewayAddress'],
                                                     "disclaimer": config['main']['disclaimer']})

@app.get('/heights')
async def getHeights():
    dbCon = sqlite.connect('gateway.db')
    result = dbCon.cursor().execute('SELECT chain, height FROM heights WHERE chain = "Other" or chain = "TN"').fetchall()
    return { result[0][0]: result[0][1], result[1][0]: result[1][1] }

@app.get('/errors')
async def getErrors(request: Request, username: str = Depends(get_current_username)):
    if (config["main"]["admin-username"] == "admin" and config["main"]["admin-password"] == "admin"):
        return {"message": "change the default username and password please!"}
    
    if username == config["main"]["admin-username"]:
        dbCon = sqlite.connect('gateway.db')
        result = dbCon.cursor().execute('SELECT * FROM errors').fetchall()
        return templates.TemplateResponse("errors.html", {"request": request, "errors": result})

@app.get('/executed')
async def getErrors(request: Request, username: str = Depends(get_current_username)):
    if (config["main"]["admin-username"] == "admin" and config["main"]["admin-password"] == "admin"):
        return {"message": "change the default username and password please!"}
    
    if username == config["main"]["admin-username"]:
        dbCon = sqlite.connect('gateway.db')
        result = dbCon.cursor().execute('SELECT * FROM executed').fetchall()
        result2 = dbCon.cursor().execute('SELECT * FROM verified').fetchall()
        return templates.TemplateResponse("tx.html", {"request": request, "txs": result, "vtxs": result2})

@app.get('/tnAddress/{address}')
async def checkTunnel(address):
    dbCon = sqlite.connect('gateway.db')
    address = re.sub('[\W_]+', '', address)
    values = (address,)

    result = dbCon.cursor().execute('SELECT sourceAddress FROM tunnel WHERE targetAddress = ?', values).fetchall()
    if len(result) == 0:
        sourceAddress = None
    else:
        sourceAddress = result[0][0]

    return { 'sourceAddress': sourceAddress, 'targetAddress': address }

@app.get('/tunnel/{targetAddress}')
async def createTunnel(targetAddress):
    dbCon = sqlite.connect('gateway.db')
    instance = authproxy.AuthServiceProxy(config['other']['node'])
    sourceAddress = instance.getnewaddress()
    targetAddress = re.sub('[\W_]+', '', targetAddress)
    values = (sourceAddress, targetAddress)

    result = dbCon.cursor().execute('SELECT sourceAddress FROM tunnel WHERE targetAddress = ?', (targetAddress,)).fetchall()
    if len(result) == 0:
        dbCon.cursor().execute('INSERT INTO TUNNEL ("sourceAddress", "targetAddress") VALUES (?, ?)', values)
        dbCon.commit()

        return { 'successful': 1, 'address': sourceAddress }
    else:
        return { 'successful': 2, 'address': result[0][0] }    

@app.get("/api/fullinfo")
async def api_fullinfo(request: Request):
    heights = await getHeights()
    tnBalance = get_tnBalance()
    otherBalance = get_otherBalance()
    return {"chainName": config['main']['name'],
            "assetID": config['tn']['assetId'],
            "tn_gateway_fee":config['tn']['gateway_fee'],
            "tn_network_fee":config['tn']['network_fee'],
            "tn_total_fee":config['tn']['network_fee']+config['tn']['gateway_fee'],
            "other_gateway_fee":config['other']['gateway_fee'],
            "other_network_fee":config['other']['network_fee'],
            "other_total_fee":config['other']['network_fee'] + config['other']['gateway_fee'],
            "fee": config['tn']['fee'],
            "company": config['main']['company'],
            "email": config['main']['contact-email'],
            "telegram": config['main']['contact-telegram'],
            "recovery_amount":config['main']['recovery_amount'],
            "recovery_fee":config['main']['recovery_fee'],
            "otherHeight": heights['Other'],
            "tnHeight": heights['TN'],
            "tnAddress": config['tn']['gatewayAddress'],
            "otherAddress": config['other']['gatewayAddress'],
            "disclaimer": config['main']['disclaimer'],
            "tn_balance": tnBalance,
            "other_balance": otherBalance,
            "minAmount": config['main']['min'],
            "maxAmount": config['main']['max'],
            "type": "tunnel"}

@app.get("/api/deposit/{tnAddress}")
async def api_depositCheck(tnAddress):
    checkit = verifier(config)
    result = checkit.checkDeposit(address=tnAddress)

    return result

@app.get("/api/wd/{tnAddress}")
async def api_wdCheck(tnAddress):
    checkit = verifier(config)
    result = checkit.checkWD(address=tnAddress)

    return result
