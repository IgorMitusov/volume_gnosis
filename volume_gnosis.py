import random
import faulthandler

from web3 import Web3
from config import cc, ANGLE, AGEUR, REFUEL, stargate_chain_code
from helper import load_contract
from utils import get_main_wallet, get_all_wallets, RPCS
import time
from oneinch import OneInch
from helper import check_approval, logger_wrapper, logger_wrapper_record
from loguru import logger
from helper import Transaction
from enum import Enum

from itertools import repeat
from pyuseragents import random as random_useragent
from requests import Session


from multiprocessing.dummy import Pool
import json
import os

class BalanceType(Enum):
    dust = 0
    small = 1,
    high = 2

def get_type_balance_native(_chain, c_balance):
    if (_chain == 'optimism' or _chain == 'arbitrum'):
        if c_balance < 0.002:
            _type_balance = BalanceType.dust
        elif c_balance > 0.01:
            _type_balance = BalanceType.high
        else:
            _type_balance = BalanceType.small
    elif _chain == 'bsc':
        if c_balance < 0.02:
            _type_balance = BalanceType.dust
        elif c_balance > 0.03:
            _type_balance = BalanceType.high
        else:
            _type_balance = BalanceType.small
    elif _chain == 'avaxc':
        if c_balance < 0.05:
            _type_balance = BalanceType.dust
        elif c_balance > 0.5:
            _type_balance = BalanceType.high
        else:
            _type_balance = BalanceType.small

    elif _chain == 'polygon':
        if c_balance < 0.03:
            _type_balance = BalanceType.dust
        elif c_balance > 2:
            _type_balance = BalanceType.high
        else:
            _type_balance = BalanceType.small
    elif _chain == 'celo':
        if c_balance < 1:
            _type_balance = BalanceType.dust
        elif c_balance > 5:
            _type_balance = BalanceType.high
        else:
            _type_balance = BalanceType.small
    elif _chain == 'harmony':
        if c_balance < 10:
            _type_balance = BalanceType.dust
        elif c_balance > 50:
            _type_balance = BalanceType.high
        else:
            _type_balance = BalanceType.small
    elif _chain == 'xDai':
        if c_balance < 0.95:
            _type_balance = BalanceType.dust
        elif c_balance > 1:
            _type_balance = BalanceType.high
        else:
            _type_balance = BalanceType.small

    return _type_balance


if os.path.exists(f"log/complete_volume.txt"):
    with open(f'{os.path.dirname(__file__)}/complete_volume.txt', 'r') as file:
        complete_volume = [row.strip() for row in file]
else:
    complete_volume = []

MAX_ATTEMPTS = 10

@check_approval
def approve_token(wallet, w3, contract_token, contract_spender, limit, amount , type, chain, *args):
    logger.info('approve token...')


def value_to_bytes(val):
    try:
        hex_val = hex(val)[2:]
    except:
        hex_val = val
    return '0' * (64 - len(hex_val)) + hex_val

def get_chain_settings(name, stable_name = ''):
    if stable_name == '':
        _st_value = random.randint(0, 1)
        if _st_value == 1:
            contract_stable = cc[name]['usdt']
        else:
            contract_stable = cc[name]['usdc']
    else:
        contract_stable = cc[name][stable_name]
    try:
        contract_native = cc[name]['native']
    except:
        contract_native = '0x0000000000000000000000000000000000000000000'

    return {'setting': RPCS[name], 'stable': contract_stable, 'native': contract_native}


def analyse(wallet):
    web3_bsc = Web3(Web3.HTTPProvider(RPCS["bsc"]["rpc"]))
    lz_bsc = ANGLE['bsc']["LayerZero"]
    lz_bsc_contract = load_contract(web3_bsc, 'angle', lz_bsc)
    lzbalance = lz_bsc_contract.functions.balanceOf(wallet["wallet"].address).call()
    if lzbalance > 0:
        logger.info(f'*******************************************')
        logger.info(f'lz token here -> {wallet["wallet"].address}')
        lz_func = lz_bsc_contract.functions.withdraw(lzbalance, wallet["wallet"].address)
        log_value = [f'\n>>> tx {RPCS["polygon"]["scan"]}/',
                     f'withdraw lz token on --> wallet {wallet["wallet"].address} ']
        Transaction.build_transaction(lz_func, web3_bsc, wallet["wallet"], 'polygon', log_value)


def wait_balance(w3, ageur_addresses, wal_address, w_balance = 0):
    if w_balance == 0:
        w_balance = 0.01

    is_agEur_amount = False
    while not is_agEur_amount:
        for _address in ageur_addresses:
            agEur_contract = load_contract(w3, 'erc20', _address)
            agEur_balance = agEur_contract.functions.balanceOf(wal_address).call()
            if agEur_balance > Web3.toWei(w_balance, 'ether'):
                logger.info(f'{Web3.fromWei(agEur_balance, "ether")} on wallet address {wal_address}')
                is_agEur_amount = True
        if not is_agEur_amount:
            logger.info(f'current balance native token < 5 on wallet --> {wal_address}, wait -60s ')
            time.sleep(60)
    return agEur_balance

def wait_balance_refuel(chain, address):
    web3 = Web3(Web3.HTTPProvider(RPCS[chain]['rpc']))
    while True:
        balance = web3.eth.get_balance(address)
        if Web3.fromWei(balance, 'ether') > 0:
            break
        logger.info(f'wait while refuel transfer balance on chain; balance - {balance} on {address}')
        time.sleep(60)

def refuel(params, chains = []):
    w3_from = Web3(Web3.HTTPProvider(RPCS[params["chain"]]["rpc"]))
    if len(chains)== 0:
        chains = ['bsc', 'avaxc', 'polygon', 'arbitrum', 'optimism']
    _amount = params["amount"]
    chainTo = []
    while True:
        if len(chains) == 0:
            break
        _ch = sorted(chains, key=lambda x: random.random())[0]
        if _ch != params['chain']:
            chainTo.append(_ch)
            chains.remove(_ch)
        else:
            if _ch == params['chain'] and len(chains) == 1:
                break

    session = Session()
    session.headers.update({
        'user-agent': random_useragent(),
        'connection': 'keep-alive',
        'content-type': 'application/json',
    })

    while True:
        try:
            refuel_resp = session.get('https://refuel.socket.tech/chains')
            refuel_chains = json.loads(refuel_resp.content)
            break
        except Exception as e:
            time.sleep(10)
            logger.info(e)

    refuel_chain = []
    for _res in refuel_chains["result"]:
        if _res["name"].lower() == params["chain"]:
            refuel_chain.append(_res)
            break

    for _ch in chainTo:
        _ch_id = RPCS[_ch]["chain_id"]
        _ch_limit = {}
        for limit in refuel_chain[0]["limits"]:
            if limit["chainId"] == _ch_id:
                ch_limit = limit
                break

        ch_max = int(ch_limit["maxAmount"])
        ch_min = int(ch_limit["minAmount"])
        if Web3.fromWei(_amount, 'ether') > Web3.fromWei(ch_max, 'ether'):
            _amount_to_refuel = random.randint(int(ch_max - ch_max / 10), ch_max)
        else:
            if params["amount_to_transfer"] != 0:
                _amount_to_refuel = Web3.toWei(params["amount_to_transfer"], 'ether')
                if _amount_to_refuel > params["amount"]:
                    _amount_to_refuel = int(params["amount"] * 0.7)
            else:
                _amount_to_refuel = int(ch_min * 10)

        w3_ch = Web3(Web3.HTTPProvider( RPCS[_ch]['rpc']))
        w3_balance = w3_ch.eth.get_balance(params["wallet"].address)
        balance_type = get_type_balance_native(_ch, Web3.fromWei(w3_balance, 'ether'))
        if balance_type != BalanceType.dust:
            logger.info(f'refuel will not send to the chain {_ch}, because there is a balance type not a dust -> {balance_type.name}')
            pass
        else:
            if params["chain"] == 'celo' or params["chain"] == 'harmony':
               pass
            else:

                refuel_contract = load_contract(w3_from, 'refuel', Web3.toChecksumAddress(REFUEL['bsc']))
                refuel_func = refuel_contract.functions.depositNativeToken(RPCS[_ch]['chain_id'], params['wallet'].address)
                log_value = [f'\n>>> tx {RPCS[params["chain"]]["scan"]}/', f'refuel deposit']
                Transaction.build_transaction(refuel_func, w3_from, params["wallet"], params["chain"], log_value, _amount_to_refuel)
                if len(chains) == 0:
                    break
    return {"chainFrom": params["chain"], "chainTo": chainTo}

def transfer_from_bsc_to_xDai(wallet, web3_bsc, idx):
    agEur_balance = wait_balance(web3_bsc, [AGEUR['bsc']], wallet["wallet"].address)

    native_balance = web3_bsc.eth.get_balance(wallet["wallet"].address)
    if native_balance < Web3.toWei(0.001, 'ether'):
        logger.info(f'native balance matic too small on wallet -{wallet["wallet"].address}')
        return True

    agEur_balance = check_balance_on_chains(web3_bsc, AGEUR['bsc'], wallet["wallet"].address, 'bsc')

    if agEur_balance['lz_agEur_balance'] > 0:
        # pait lz
        #logger.info(f'lz balance on wallet --> {wallet["wallet"].address}  --> {agEur_balance["lz_agEur_balance"]}')
        #inch = OneInch('0x0c1EBBb61374dA1a8C57cB6681bF27178360d36F', AGEUR['polygon'], True, RPCS["polygon"])
        #inch.swap(wallet["wallet"], web3_matic,  agEur_balance['lz_agEur_balance'], 1, 1)
        logger.info(f"**********************************************************************************************")
        logger.info(f"lp token on wallet -> {wallet['wallet'].address}")
        logger.info(f"**********************************************************************************************")

        lz_matic = ANGLE['polygon']["LayerZero"]
        lz_matic_contract = load_contract(web3_bsc, 'angle', lz_matic)
        lzbalance = lz_matic_contract.functions.balanceOf(wallet["wallet"].address).call()
        if lzbalance > 0:
            logger.info(f'*******************************************')
            logger.info(f'lz token here -> {wallet["wallet"].address}')
            lz_func = lz_matic_contract.functions.withdraw(lzbalance, wallet["wallet"].address)
            log_value = [f'\n>>> tx {RPCS["polygon"]["scan"]}/',
                         f'withdraw lz token on --> wallet {wallet["wallet"].address} ']
            Transaction.build_transaction(lz_func, web3_bsc, wallet["wallet"], 'polygon', log_value)

    if agEur_balance['balance'] > 0:

        agEur_balance = check_balance_on_chains(web3_bsc, AGEUR['bsc'], wallet["wallet"].address, 'bsc')

        lz_bsc = ANGLE['bsc']["LayerZero"]
        lz_bsc_contract = load_contract(web3_bsc, 'angle', lz_bsc)
        commision = lz_bsc_contract.functions.estimateSendFee(stargate_chain_code['celo'],
                                                                Web3.toBytes(hexstr=wallet["wallet"].address),
                                                                agEur_balance['balance'],
                                                                False,
                                                                Web3.toBytes(
                                                                    hexstr='0x000100000000000000000000000000000000000000000000000000000000000493e0')
                                                                ).call()


        approve_token(wallet["wallet"], web3_bsc, AGEUR["bsc"], lz_bsc, int(f"0x{64 * 'f'}", 16),
                      agEur_balance['balance'],
                      'erc20', 'bsc')

        lz_func = lz_bsc_contract.functions.send(stargate_chain_code['xDai'], wallet["wallet"].address,
                                                   agEur_balance['balance'], wallet["wallet"].address,
                                                   '0x0000000000000000000000000000000000000000',
                                                   Web3.toBytes(
                                                       hexstr='0x000100000000000000000000000000000000000000000000000000000000000493e0'))
        log_value = [f'\n>>> tx {RPCS["bsc"]["scan"]}/', f'transfer agEur to celo --> wallet {wallet["wallet"].address} / left to do --> {idx}']
        Transaction.build_transaction(lz_func, web3_bsc, wallet["wallet"], 'bsc', log_value, commision[0])
        return False
    else:
        return True

def transfer_from_xDai_to_bsc(wallet, web3_xDai, idx):
    agEur_balance = wait_balance(web3_xDai, [AGEUR['xDai'], '0xfa5ed56a203466cbbc2430a43c66b9d8723528e7'], wallet["wallet"].address)

    native_balance = web3_xDai.eth.get_balance(wallet["wallet"].address)
    if native_balance < Web3.toWei(0.01, 'ether'):
        logger.info(f'native balance matic too small on wallet -{wallet["wallet"].address}')
        return True

    agEur_balance = check_balance_on_chains(web3_xDai, AGEUR['xDai'], wallet["wallet"].address, 'xDai')

    if agEur_balance['lz_agEur_balance'] > 0:
        # pait lz
        #logger.info(f'lz balance on wallet --> {wallet["wallet"].address}  --> {agEur_balance["lz_agEur_balance"]}')
        #inch = OneInch('0x0c1EBBb61374dA1a8C57cB6681bF27178360d36F', AGEUR['polygon'], True, RPCS["polygon"])
        #inch.swap(wallet["wallet"], web3_matic,  agEur_balance['lz_agEur_balance'], 1, 1)
        logger.info(f"**********************************************************************************************")
        logger.info(f"lp token on wallet -> {wallet['wallet'].address}")
        logger.info(f"**********************************************************************************************")

        lz_xDai = ANGLE['xDai']["LayerZero"]
        lz_xDai_contract = load_contract(web3_xDai, 'angle', lz_xDai)
        lzbalance = lz_xDai_contract.functions.balanceOf(wallet["wallet"].address).call()
        if lzbalance > 0:
            logger.info(f'*******************************************')
            logger.info(f'lz token here -> {wallet["wallet"].address}')
            lz_func = lz_xDai_contract.functions.withdraw(lzbalance, wallet["wallet"].address)
            log_value = [f'\n>>> tx {RPCS["xDai"]["scan"]}/',
                         f'withdraw lz token on --> wallet {wallet["wallet"].address} ']
            Transaction.build_transaction(lz_func, web3_xDai, wallet["wallet"], 'xDai', log_value)

    if agEur_balance['balance'] > 0:

        agEur_balance = check_balance_on_chains(web3_xDai, AGEUR['xDai'], wallet["wallet"].address, 'xDai')

        lz_xDai = ANGLE['xDai']["LayerZero"]
        lz_matic_contract = load_contract(web3_xDai, 'angle', lz_xDai)
        commision = lz_matic_contract.functions.estimateSendFee(stargate_chain_code['bsc'],
                                                                Web3.toBytes(hexstr=wallet["wallet"].address),
                                                                agEur_balance['balance'],
                                                                False,
                                                                Web3.toBytes(
                                                                    hexstr='0x000100000000000000000000000000000000000000000000000000000000000493e0')
                                                                ).call()


        approve_token(wallet["wallet"], web3_xDai, AGEUR["xDai"], lz_xDai, int(f"0x{64 * 'f'}", 16),
                      agEur_balance['balance'],
                      'erc20', 'xDai')

        lz_func = lz_matic_contract.functions.send(stargate_chain_code['bsc'], wallet["wallet"].address,
                                                   agEur_balance['balance'], wallet["wallet"].address,
                                                   '0x0000000000000000000000000000000000000000',
                                                   Web3.toBytes(
                                                       hexstr='0x000100000000000000000000000000000000000000000000000000000000000493e0'))
        log_value = [f'\n>>> tx {RPCS["xDai"]["scan"]}/', f'transfer agEur to bsc --> wallet {wallet["wallet"].address} / left to do --> {idx}']
        Transaction.build_transaction(lz_func, web3_xDai, wallet["wallet"], 'xDai', log_value, commision[0])

        return False
    else:
        return True

def transfer_from_xDai_to_celo(wallet, web3_xDai, idx):
    agEur_balance = wait_balance(web3_xDai, [AGEUR['xDai'], '0xfa5ed56a203466cbbc2430a43c66b9d8723528e7'], wallet["wallet"].address)

    native_balance = web3_xDai.eth.get_balance(wallet["wallet"].address)
    if native_balance < Web3.toWei(0.01, 'ether'):
        logger.info(f'native balance matic too small on wallet -{wallet["wallet"].address}')
        return True

    agEur_balance = check_balance_on_chains(web3_xDai, AGEUR['xDai'], wallet["wallet"].address, 'xDai')

    if agEur_balance['lz_agEur_balance'] > 0:
        # pait lz
        #logger.info(f'lz balance on wallet --> {wallet["wallet"].address}  --> {agEur_balance["lz_agEur_balance"]}')
        #inch = OneInch('0x0c1EBBb61374dA1a8C57cB6681bF27178360d36F', AGEUR['polygon'], True, RPCS["polygon"])
        #inch.swap(wallet["wallet"], web3_matic,  agEur_balance['lz_agEur_balance'], 1, 1)
        logger.info(f"**********************************************************************************************")
        logger.info(f"lp token on wallet -> {wallet['wallet'].address}")
        logger.info(f"**********************************************************************************************")

        lz_xDai = ANGLE['xDai']["LayerZero"]
        lz_xDai_contract = load_contract(web3_xDai, 'angle', lz_xDai)
        lzbalance = lz_xDai_contract.functions.balanceOf(wallet["wallet"].address).call()
        if lzbalance > 0:
            logger.info(f'*******************************************')
            logger.info(f'lz token here -> {wallet["wallet"].address}')
            lz_func = lz_xDai_contract.functions.withdraw(lzbalance, wallet["wallet"].address)
            log_value = [f'\n>>> tx {RPCS["xDai"]["scan"]}/',
                         f'withdraw lz token on --> wallet {wallet["wallet"].address} ']
            Transaction.build_transaction(lz_func, web3_xDai, wallet["wallet"], 'xDai', log_value)

    if agEur_balance['balance'] > 0:

        agEur_balance = check_balance_on_chains(web3_xDai, AGEUR['xDai'], wallet["wallet"].address, 'xDai')

        lz_xDai = ANGLE['xDai']["LayerZero"]
        lz_matic_contract = load_contract(web3_xDai, 'angle', lz_xDai)
        commision = lz_matic_contract.functions.estimateSendFee(stargate_chain_code['celo'],
                                                                Web3.toBytes(hexstr=wallet["wallet"].address),
                                                                agEur_balance['balance'],
                                                                False,
                                                                Web3.toBytes(
                                                                    hexstr='0x000100000000000000000000000000000000000000000000000000000000000493e0')
                                                                ).call()


        approve_token(wallet["wallet"], web3_xDai, AGEUR["xDai"], lz_xDai, int(f"0x{64 * 'f'}", 16),
                      agEur_balance['balance'],
                      'erc20', 'xDai')

        lz_func = lz_matic_contract.functions.send(stargate_chain_code['celo'], wallet["wallet"].address,
                                                   agEur_balance['balance'], wallet["wallet"].address,
                                                   '0x0000000000000000000000000000000000000000',
                                                   Web3.toBytes(
                                                       hexstr='0x000100000000000000000000000000000000000000000000000000000000000493e0'))
        log_value = [f'\n>>> tx {RPCS["xDai"]["scan"]}/', f'transfer agEur to celo --> wallet {wallet["wallet"].address} / left to do --> {idx}']
        Transaction.build_transaction(lz_func, web3_xDai, wallet["wallet"], 'xDai', log_value, commision[0])

        return False
    else:
        return True

def transfer_from_celo_to_xDai(wallet, web3_celo, idx, w_balance=0):

    agEur_balance = wait_balance(web3_celo, [AGEUR['celo']], wallet["wallet"].address, w_balance)
    logger.info(Web3.fromWei(agEur_balance, "ether"))

    native_balance = web3_celo.eth.get_balance(wallet["wallet"].address)
    if native_balance < Web3.toWei(0.01, 'ether'):
        logger.info(f'native balance celo too small on wallet -{wallet["wallet"].address}')
        return True

    if agEur_balance > 0:
        lz_celo = Web3.toChecksumAddress('0xf1ddcaca7d17f8030ab2eb54f2d9811365efe123')
        lz_celo_contract = load_contract(web3_celo, 'angle', lz_celo)


        commision = lz_celo_contract.functions.estimateSendFee(stargate_chain_code['xDai'],
                                                                Web3.toBytes(hexstr=wallet["wallet"].address),
                                                                agEur_balance,
                                                                False,
                                                                Web3.toBytes(
                                                                    hexstr='0x000100000000000000000000000000000000000000000000000000000000000493e0')
                                                                ).call()

        approve_token(wallet["wallet"], web3_celo, AGEUR["celo"], lz_celo, int(f"0x{64 * 'f'}", 16),
                      agEur_balance,
                      'erc20', 'celo')

        lz_func = lz_celo_contract.functions.send(stargate_chain_code['xDai'], wallet["wallet"].address,
                                                   agEur_balance, wallet["wallet"].address,
                                                   '0x0000000000000000000000000000000000000000',
                                                   Web3.toBytes(
                                                       hexstr='0x000100000000000000000000000000000000000000000000000000000000000493e0'))

        log_value = [f'\n>>> tx {RPCS["celo"]["scan"]}/',
                     f'transfer agEur to celo --> wallet {wallet["wallet"].address} / idx --> {idx}']
        Transaction.build_transaction(lz_func, web3_celo, wallet["wallet"], 'xDai', log_value, commision[0])

        return False
    else:
        return True

def check_balance_on_chains(w3, ageur_address, wal_address, chain_name):
    native_balance = w3.eth.get_balance(wal_address)
    agEur_contract = load_contract(w3, 'erc20', ageur_address)
    agEur_balance = agEur_contract.functions.balanceOf(wal_address).call()
    if chain_name == 'polygon':
        lz_agEur_contract = load_contract(w3, 'erc20', '0x0c1EBBb61374dA1a8C57cB6681bF27178360d36F')
        lz_agEur_balance = lz_agEur_contract.functions.balanceOf(wal_address).call()
    elif chain_name == 'xDai':
        lz_agEur_contract = load_contract(w3, 'erc20', '0xfa5ed56a203466cbbc2430a43c66b9d8723528e7')
        lz_agEur_balance = lz_agEur_contract.functions.balanceOf(wal_address).call()
    else:
        lz_agEur_balance = 0

    return {"chain": chain_name, "balance": agEur_balance, 'native': native_balance, 'lz_agEur_balance': lz_agEur_balance}


@logger_wrapper
def task_refuel(wallet):
    web3_xDai = Web3(Web3.HTTPProvider(RPCS["xDai"]["rpc"]))
    xDai_balance = web3_xDai.eth.get_balance(wallet["wallet"].address)
    web3_bsc = Web3(Web3.HTTPProvider(RPCS["bsc"]["rpc"]))
    bsc_balance = web3_bsc.eth.get_balance(wallet["wallet"].address)

    logger.info(f'xDai balance on wallet - {wallet["wallet"].address}  ->  {Web3.fromWei(xDai_balance, "ether")}')
    if Web3.fromWei(xDai_balance, 'ether') < 0.95:
        params = {
            "wallet": wallet["wallet"],
            "chain": 'bsc',
            "amount": bsc_balance,
            "amount_to_transfer": round(random.uniform(0.002, 0.005), 4)
        }


        refuel_path = refuel(params, chains=['xDai'])
        #wait_balance_refuel(refuel_path["chainTo"][0], wallet["wallet"].address)
        #wait_balance_refuel('xDai', wallet["wallet"].address)

@logger_wrapper
def task_swap_to_ag_euro(wallet):
    logger.info(f'{wallet["wallet"].address}')
    web3_bsc = Web3(Web3.HTTPProvider(RPCS["bsc"]["rpc"]))
    bsc_balance = web3_bsc.eth.get_balance(wallet["wallet"].address)

    wait_balance_refuel('xDai', wallet["wallet"].address)

    while True:
        ageur_contract = load_contract(web3_bsc, 'erc20', AGEUR['bsc'])
        ag_balance = ageur_contract.functions.balanceOf(wallet["wallet"].address).call()
        if Web3.fromWei(ag_balance, 'ether') > 0.01:
            break

        web3_xDai = Web3(Web3.HTTPProvider(RPCS["xDai"]["rpc"]))
        ageur_contract = load_contract(web3_xDai, 'erc20', AGEUR['xDai'])
        ag_balance = ageur_contract.functions.balanceOf(wallet["wallet"].address).call()
        if Web3.fromWei(ag_balance, 'ether') > 0.01:
            break

        web3_celo = Web3(Web3.HTTPProvider(RPCS["celo"]["rpc"]))
        ageur_contract = load_contract(web3_celo, 'erc20', AGEUR['celo'])
        ag_balance = ageur_contract.functions.balanceOf(wallet["wallet"].address).call()
        if Web3.fromWei(ag_balance, 'ether') > 0.01:
            break

        logger.info(f'bsc balance - {bsc_balance}')
        if bsc_balance > Web3.toWei(0.005, 'ether'):
            _agMinEur = min(bsc_balance, Web3.toWei(round(random.uniform(0.005, 0.007), 4), 'ether'))
            if _agMinEur == bsc_balance:
                amount = _agMinEur / 2
            else:
                amount = bsc_balance - (Web3.toWei(round(random.uniform(0.005, 0.007), 4), 'ether'))
            oneinch = OneInch(cc["bsc"]["native"], AGEUR['bsc'], False, RPCS["bsc"])
            oneinch.swap(wallet["wallet"], web3_bsc, int(amount),1,1)

            time.sleep(10)
            ageur_contract = load_contract(web3_bsc, 'erc20', AGEUR['bsc'])
            ag_balance = ageur_contract.functions.balanceOf(wallet["wallet"].address).call()
            if ag_balance > 0:
                break
            logger.info(f'ag_euro balance - {ag_balance}')
        else:
            logger.info(f'bsc_balance on wallet -> {wallet["wallet"].address} less than minimum - 0.005')
            break

@logger_wrapper
def task_found_route_path(wallet):
    logger.info(f'start -> {wallet["wallet"].address}')
    web3_bsc = Web3(Web3.HTTPProvider(RPCS["bsc"]["rpc"]))
    bsc_balance = web3_bsc.eth.get_balance(wallet["wallet"].address)
    web3_celo = Web3(Web3.HTTPProvider(RPCS["celo"]["rpc"]))
    web3_xDai = Web3(Web3.HTTPProvider(RPCS["xDai"]["rpc"]))

    celo_nonce = web3_celo.eth.get_transaction_count(wallet["wallet"].address)
    if celo_nonce > MAX_ATTEMPTS:
        agEur_balance = check_balance_on_chains(web3_bsc, AGEUR['bsc'], wallet["wallet"].address, 'bsc')[
            "balance"]

        isCompleteVolume = False
        for complete_wallet in complete_volume:
            if complete_wallet == wallet["wallet"].address:
                isCompleteVolume = True

        if not isCompleteVolume:
            with open(f'complete_volume.txt', 'a+') as file:
                file.write(f'{wallet["wallet"].address.lower()}\n')

        token = ['usdc', 'usdt']
        if agEur_balance > 0:
            oneinch = OneInch(AGEUR['bsc'], cc["bsc"][random.choice(token)], True, RPCS["bsc"])
            oneinch.swap(wallet["wallet"], web3_bsc, agEur_balance, 1, 1)

        logger.info(f' complete task on wallet --> {wallet["wallet"].address} --> wallet {wallet["wallet"].address}')
    else:
        #bsc_balance = web3_bsc.eth.get_balance(wallet["wallet"].address)
        pathes = []
        if os.path.exists(f'logs_pathes/{wallet["wallet"].address}.txt'):
            with open(f'{os.path.dirname(__file__)}/logs_pathes/{wallet["wallet"].address}.txt', 'r') as file:
                pathes = [row.strip() for row in file]

        if not len(pathes) > 0:

            chains = ["bsc", 'xDai', "celo"]
            c_balances = []
            for _ch in chains:
                c_balances.append(
                    check_balance_on_chains(Web3(Web3.HTTPProvider(RPCS[_ch]["rpc"])), AGEUR[_ch], wallet["wallet"].address,_ch))

            first_chain_for_swap = None

            for c_balance in c_balances:
                logger.info(f'agEur balance - {c_balance["balance"]} native balance - {c_balance["native"]}')
                if c_balance["chain"] == 'bsc' and c_balance['balance'] > 0:
                    first_chain_for_swap = c_balance["chain"]
                    break
                if c_balance["chain"] == 'xDai' and (c_balance['balance'] > 0 or c_balance["lz_agEur_balance"] > 0):
                  first_chain_for_swap = c_balance["chain"]
                  break
                if c_balance["chain"] == 'celo' and (c_balance['balance'] > 0 or c_balance["lz_agEur_balance"] > 0):
                  first_chain_for_swap = c_balance["chain"]
                  break

            path_to_swap = [first_chain_for_swap]

            if path_to_swap[0] == None:
                logger.info(f'1inch swap on wallet -{wallet["wallet"].address} rejected')
                with open(f'not_swap_wallet.txt', 'a+') as file:
                    file.write(f'{wallet["wallet"].address.lower()}\n')
            else:
                attempts = random.randint(MAX_ATTEMPTS - 3, MAX_ATTEMPTS)
                attempts = attempts - celo_nonce

                if attempts <= 0:
                    logger.info(f'number of attempts - {attempts} on wallet --> {wallet["wallet"].address}')
                else:
                    for idx in range(attempts):
                        if path_to_swap[0] == 'bsc' and idx == 0:
                            path_to_swap.append('xDai')
                        #elif idx == (attempts - 1):
                        #    path_to_swap.append('celo')
                        else:
                            if path_to_swap[0] == 'celo' and idx == 0:
                                path_to_swap.append('xDai')
                            else:
                                path_to_swap.append('celo')
                                path_to_swap.append('xDai')

                    with open(f'logs_pathes/{wallet["wallet"].address}.txt', 'a+') as file:
                        for _path in path_to_swap:
                            file.write(f'{_path}\n')

def task_crossswap(wallet):
    try:
        with open(f'{os.path.dirname(__file__)}/logs_pathes/{wallet["wallet"].address}.txt', 'r') as file:
            pathes = [row.strip() for row in file]

        if len(pathes) > 0:
            path_to_swap = pathes[0]
            errors = False
            logger.info(f'path to wallet --> {wallet["wallet"].address} --> {path_to_swap}')

            if path_to_swap == 'bsc':
                web3_bsc = Web3(Web3.HTTPProvider(RPCS["bsc"]["rpc"]))
                errors = transfer_from_bsc_to_xDai(wallet, web3_bsc, len(pathes))
            elif len(pathes) == 1 and path_to_swap == 'xDai':
                web3_xDai = Web3(Web3.HTTPProvider(RPCS["xDai"]["rpc"]))
                errors = transfer_from_xDai_to_bsc(wallet, web3_xDai, len(pathes))
            elif path_to_swap == 'xDai':
                web3_xDai = Web3(Web3.HTTPProvider(RPCS["xDai"]["rpc"]))
                errors = transfer_from_xDai_to_celo(wallet, web3_xDai, len(pathes))
            else:
                web3_celo = Web3(Web3.HTTPProvider(RPCS["celo"]["rpc"]))
                errors = transfer_from_celo_to_xDai(wallet, web3_celo, len(pathes))

        if errors == False:
            pathes.pop(0)

            with open(f'logs_pathes/{wallet["wallet"].address}.txt', 'w') as file:
                for _path in pathes:
                    file.write(f'{_path}\n')

    except Exception as e:
        logger.error(e)


@logger_wrapper
def task_check_ag_on_wallets(wallet):
    web3_bsc = Web3(Web3.HTTPProvider(RPCS["bsc"]["rpc"]))
    web3_xDai = Web3(Web3.HTTPProvider(RPCS["xDai"]["rpc"]))
    web3_celo = Web3(Web3.HTTPProvider(RPCS["celo"]["rpc"]))
    address = wallet["wallet"].address
    logger.info(f'complete pathes on wallet --> {address}')

    agEur_contract_bsc = load_contract(web3_bsc, 'erc20', AGEUR['bsc'])
    agEur_contract_xDai = load_contract(web3_xDai, 'erc20', AGEUR['xDai'])
    agEur_contract_celo = load_contract(web3_celo, 'erc20', AGEUR['celo'])
    agEur_balance_bsc = agEur_contract_bsc.functions.balanceOf(address).call()
    agEur_balance_xDai = agEur_contract_xDai.functions.balanceOf(address).call()
    agEur_balance_celo = agEur_contract_celo.functions.balanceOf(address).call()


    if Web3.fromWei(agEur_balance_bsc, 'ether') > 0 and Web3.fromWei(agEur_balance_xDai, 'ether') < 0.1 and Web3.fromWei(agEur_balance_celo, 'ether') < 0.1:
        logger.info(
            f'balance agEur: bsc -> {agEur_balance_bsc}; xDai -> {agEur_balance_xDai}; celo -> {agEur_balance_celo}')
        token = ['usdc', 'usdt']

        oneinch = OneInch(AGEUR['bsc'], cc["bsc"][random.choice(token)], True, RPCS["bsc"])
        oneinch.swap(wallet["wallet"], web3_bsc, agEur_balance_bsc, 1, 1)

        agEur_balance = agEur_contract_bsc.functions.balanceOf(address).call()
        if Web3.fromWei(agEur_balance, 'ether') < 0.0001:
            with open(f'logs/task_swap_from_ag_euro.txt', 'a+') as file:
                file.write(f'{wallet["wallet"].address.lower()}\n')

@logger_wrapper_record
def task_swap_from_ag_euro(wallet):
    web3_bsc = Web3(Web3.HTTPProvider(RPCS["bsc"]["rpc"]))

    logger.info(f'complete pathes on wallet --> {wallet["wallet"].address}')
    agEur_balance = wait_balance(web3_bsc, [AGEUR['bsc']], wallet["wallet"].address)

    token = ['usdc', 'usdt']

    oneinch = OneInch(AGEUR['bsc'], cc["bsc"][random.choice(token)], True, RPCS["bsc"])
    oneinch.swap(wallet["wallet"], web3_bsc, agEur_balance, 1, 1)
    logger.info(
        f' complete task on wallet --> {wallet["wallet"].address} --> wallet {wallet["wallet"].address}')

if __name__ == '__main__':
    faulthandler.enable()
    wallets = get_all_wallets(get_main_wallet())

    wallets_to_swap = []

    if os.path.exists(f"logs/task_swap_from_ag_euro.txt"):
        with open(f'{os.path.dirname(__file__)}/logs/task_swap_from_ag_euro.txt', 'r') as file:
            wallets_complete = [row.strip() for row in file]
    else:
        wallets_complete = []

    #unique = [x for x in  wallets_complete if  wallets_complete.count(x) == 1]

    multith = str(input("multithreading? - y/n \n"))
    if multith == 'Y' or multith == 'y':
        threads = int(input("number of threads? \n"))
    else:
        threads = 1
    random.shuffle(wallets_to_swap)
    pool = Pool(threads)

    pool.map(task_check_ag_on_wallets, wallets)
    pool.map(analyse, wallets)
    pool.map(task_swap_to_ag_euro, wallets_to_swap)

    if os.path.exists(f"logs/task_swap_from_ag_euro.txt"):
        with open(f'{os.path.dirname(__file__)}/logs/task_swap_from_ag_euro.txt', 'r') as file:
            wallets_complete = [row.strip() for row in file]

    for wal in wallets:
        isWalletComplete = False
        for wallet_complete in wallets_complete:
            if wal["wallet"].address.lower() == wallet_complete.lower():
                isWalletComplete = True
                break
        if not isWalletComplete:
            wallets_to_swap.append(wal)

    isFirstTaskComplete = False
    _temp_walelts_to_swap = []

    for _w in wallets_to_swap:
        logger.info(_w["wallet"].address)


    for _w in wallets_to_swap:
        address = _w["wallet"].address
        if os.path.exists(f"logs_pathes/{address}.txt"):
            with open(f'{os.path.dirname(__file__)}/logs_pathes/{address}.txt', 'r') as file:
                routes = [row.strip() for row in file]
            if len(routes) > 0:
                _temp_walelts_to_swap.append(_w)
        else:
            _temp_walelts_to_swap.append(_w)

    pool.map(task_refuel, _temp_walelts_to_swap)
    pool.map(task_swap_to_ag_euro, _temp_walelts_to_swap)
    pool.map(task_found_route_path, _temp_walelts_to_swap)

    wallets_to_swap = _temp_walelts_to_swap

    '''
    temp = []
    for _wal in wallets_to_swap:
        if _wal["wallet"].address.lower() == '0xBe01c52A1be6A235D57B08Eb9244637B9c5e3973':
            temp.append(_wal)

    wallets_to_swap = temp
    '''

    while True:
        _new_wallets_to_swap = []
        for wallet_to_swap in wallets_to_swap:
            if os.path.exists(f"logs_pathes/{wallet_to_swap['wallet'].address}.txt"):
                with open(f'{os.path.dirname(__file__)}/logs_pathes/{wallet_to_swap["wallet"].address}.txt', 'r') as file:
                    pathes = [row.strip() for row in file]

            if len(pathes) > 0:
                _new_wallets_to_swap.append(wallet_to_swap)

        pool.map(task_crossswap, _new_wallets_to_swap)

        if len(_new_wallets_to_swap) == 0:
            break

    pool.map(task_swap_from_ag_euro, wallets_to_swap)
    pool.map(task_check_ag_on_wallets, wallets)


    logger.info('complete all tasks')


