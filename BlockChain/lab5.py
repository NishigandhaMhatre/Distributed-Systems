# -*- coding: utf-8 -*-
"""
Created on Sat Nov 30 21:36:17 2019
@author: Nishigandha Mhatre
Seattle University
Assignment 5: Blockchain

    The program follows below sequence:
    1. Sends Version message
    2. Send Verack after receiving version from peer
    3. Send getheaders
    4. Download header hashes
    5. Request transactions from your block
    6. Read the block message
    7. Modify and broadcast your block 
    8. Peer rejects the message
"""


import random
import hashlib
import socket
import time
import binascii
from time import gmtime, strftime



HDR_SZ = 24
PEER_ADDR = ("46.229.165.138",8333)
BLOCK_SIZE = 1024
MICROS_PER_SECOND = 1_000_000
SUID = 1741 

def compactsize_t(n):
    if n < 252:
        return uint8_t(n)
    if n < 0xffff:
        return uint8_t(0xfd) + uint16_t(n)
    if n < 0xffffffff:
        return uint8_t(0xfe) + uint32_t(n)
    return uint8_t(0xff) + uint64_t(n)


def unmarshal_compactsize(b):
    key = b[0]
    if key == 0xff:
        return b[0:9], unmarshal_uint(b[1:9])
    if key == 0xfe:
        return b[0:5], unmarshal_uint(b[1:5])
    if key == 0xfd:
        return b[0:3], unmarshal_uint(b[1:3])
    return b[0:1], unmarshal_uint(b[0:1])


def bool_t(flag):
    return uint8_t(1 if flag else 0)


def ipv6_from_ipv4(ipv4_str):
    pchIPv4 = bytearray([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff])
    return pchIPv4 + bytearray((int(x) for x in ipv4_str.split('.')))


def ipv6_to_ipv4(ipv6):
    return '.'.join([str(b) for b in ipv6[12:]])


def uint8_t(n):
    return int(n).to_bytes(1, byteorder='little', signed=False)


def uint16_t(n):
    return int(n).to_bytes(2, byteorder='little', signed=False)


def int32_t(n):
    return int(n).to_bytes(4, byteorder='little', signed=True)


def uint32_t(n):
    return int(n).to_bytes(4, byteorder='little', signed=False)


def int64_t(n):
    return int(n).to_bytes(8, byteorder='little', signed=True)


def uint64_t(n):
    return int(n).to_bytes(8, byteorder='little', signed=False)


def unmarshal_int(b):
    return int.from_bytes(b, byteorder='little', signed=True)


def unmarshal_uint(b):
    return int.from_bytes(b, byteorder='little', signed=False)

def reverse(val):
    newval = ""
    index = len(val)-1
    while(index > 0):
        offset =  val[index-1] + val[index]
        newval += offset
        index = index-2
    return newval

def print_message(msg, text=None):
    """
    Report the contents of the given bitcoin message
    :param msg: bitcoin message including header
    :return: message type
    """
    print('\n{}MESSAGE'.format('' if text is None else (text + ' ')))
    print('({}) {}'.format(len(msg), msg[:60].hex() + ('' if len(msg) < 60 else '...')))
    payload = msg[HDR_SZ:]
    command = print_header(msg[:HDR_SZ], checksum(payload))
    if command == 'version':
        print_version_msg(payload)
    # FIXME print out the payloads of other types of messages, too
    return command


def print_version_msg(b):
    """
    Report the contents of the given bitcoin version message (sans the header)
    :param payload: version message contents
    """
    # pull out fields
    version, my_services, epoch_time, your_services = b[:4], b[4:12], b[12:20], b[20:28]
    rec_host, rec_port, my_services2, my_host, my_port = b[28:44], b[44:46], b[46:54], b[54:70], b[70:72]
    nonce = b[72:80]
    user_agent_size, uasz = unmarshal_compactsize(b[80:])
    i = 80 + len(user_agent_size)
    user_agent = b[i:i + uasz]
    i += uasz
    start_height, relay = b[i:i + 4], b[i + 4:i + 5]
    extra = b[i + 5:]

    # print report
    prefix = '  '
    print(prefix + 'VERSION')
    print(prefix + '-' * 56)
    prefix *= 2
    print('{}{:32} version {}'.format(prefix, version.hex(), unmarshal_int(version)))
    print('{}{:32} my services'.format(prefix, my_services.hex()))
    time_str = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime(unmarshal_int(epoch_time)))
    print('{}{:32} epoch time {}'.format(prefix, epoch_time.hex(), time_str))
    print('{}{:32} your services'.format(prefix, your_services.hex()))
    print('{}{:32} your host {}'.format(prefix, rec_host.hex(), ipv6_to_ipv4(rec_host)))
    print('{}{:32} your port {}'.format(prefix, rec_port.hex(), unmarshal_uint(rec_port)))
    print('{}{:32} my services (again)'.format(prefix, my_services2.hex()))
    print('{}{:32} my host {}'.format(prefix, my_host.hex(), ipv6_to_ipv4(my_host)))
    print('{}{:32} my port {}'.format(prefix, my_port.hex(), unmarshal_uint(my_port)))
    print('{}{:32} nonce'.format(prefix, nonce.hex()))
    print('{}{:32} user agent size {}'.format(prefix, user_agent_size.hex(), uasz))
    print('{}{:32} user agent \'{}\''.format(prefix, user_agent.hex(), str(user_agent, encoding='utf-8')))
    print('{}{:32} start height {}'.format(prefix, start_height.hex(), unmarshal_uint(start_height)))
    print('{}{:32} relay {}'.format(prefix, relay.hex(), bytes(relay) != b'\0'))
    if len(extra) > 0:
        print('{}{:32} EXTRA!!'.format(prefix, extra.hex()))

def print_dataPayload(b):
    """
    Method to print header hash received in the headers message
    """
    version, prev_block_hash, merkle_hashroot, time, nBits, nounce = b[:4],b[4:36],b[36:68],b[68:72],b[72:76],b[76:80]
    prefix = ' '
    print(prefix + 'BLOCK HEADER')
    print(prefix + '-' * 56)
    prefix *=2
    print('{}{:32} version {}'.format(prefix, version.hex(), unmarshal_int(version)))
    print('{}previous block hash          {}'.format(prefix,reverse(str(prev_block_hash.hex()))))
    print('{}merkle root                  {}'.format(prefix, reverse(str(merkle_hashroot.hex()))))
    time_str = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime(unmarshal_int(time)))
    print('{}{:32} epoch time {}'.format(prefix, time.hex(), time_str))
    print('{}{:32} nBits {}'.format(prefix, nBits.hex(), unmarshal_int(nBits)))
    print('{}{:32} nounce {}'.format(prefix, nounce.hex(), unmarshal_int(nounce)))
    return reverse(str(prev_block_hash.hex()))


def print_txns(b):
    """
    Method to decode and print transactions in block message
    Returns the length of the transaction
    """
    version = b[:4] 
    offset = 4
    tx_in_count = unmarshal_compactsize(b[offset:])
    offset +=len(tx_in_count[0])
    #decoding txIN
    previous_output = b[offset :offset+36]
    previous_hash = previous_output[:32]
    previous_index = previous_output[32:]
    offset +=36
    script_bytes = unmarshal_compactsize(b[offset:])
    offset +=len(script_bytes[0])
    signature = b[offset : offset + script_bytes[1]]
    offset += script_bytes[1]
    sequence = b[offset:offset+4]
    offset+= 4
    #End of TxIN    
    txn_out_count = unmarshal_compactsize(b[offset:])
    offset +=len(txn_out_count[0])
    #decoding transation out
    value = b[offset : offset+8]
    offset+=8
    pk_script_bytes = unmarshal_compactsize(b[offset:])
    offset += len(pk_script_bytes[0])
    pk_script = b[offset : offset + pk_script_bytes[1]]
    offset+= pk_script_bytes[1]
    #end of txnOut
    epoch_time = b[offset:offset+4]
    offset += 4
    prefix = ' '
    print(prefix + '=' * 100)
    print(prefix +  ' ' * 40 + 'BLOCK TRANSACTIONS')
    print(prefix + '=' * 100)
    prefix *=2
    print('{}{:32} version {}'.format(prefix, version.hex(), unmarshal_int(version)))
    print('{}tx_in_count                      {}'.format(prefix, tx_in_count[1]))
    print(('-' * 45 + 'INCOMING TX INFO' + '-' * 40).format(prefix))
    print('{}previous output hash             {}'.format(prefix, previous_hash.hex()))
    print('{}previous output index            {}'.format(prefix, unmarshal_uint(previous_index)))
    print('{}number of bytes in signature     {}'.format(prefix, script_bytes[1]))
    print('{}Signature                        {}'.format(prefix, signature.hex()))
    print('{}sequence                         {} {}'.format(prefix, sequence.hex(), unmarshal_uint(sequence)))
    print('{}tx_out_count                     {}'.format(prefix, txn_out_count[1]))
    print(('-' * 38 + 'OUTGOING INCOMING TX INFO' +'-' * 38).format(prefix))
    print('{}{:32} value {}'.format(prefix, value.hex(), unmarshal_int(value)))
    print('{}Number of bytes in pubkey        {}'.format(prefix, pk_script_bytes[1]))
    print('{}Public key Script                {}'.format(prefix, pk_script.hex()[:32]))
    time_str = strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime(unmarshal_int(epoch_time)))
    print('{}{:32} lock time {}'.format(prefix, epoch_time.hex(), time_str))
    return offset
 
    
def print_header(header, expected_cksum=None):
    """
    Report the contents of the given bitcoin message header
    :param header: bitcoin message header (bytes or bytearray)
    :param expected_cksum: the expected checksum for this version message, if known
    :return: message type
    """
    magic, command_hex, payload_size, cksum = header[:4], header[4:16], header[16:20], header[20:]
    command = str(bytearray([b for b in command_hex if b != 0]), encoding='utf-8')
    psz = unmarshal_uint(payload_size)
    if expected_cksum is None:
        verified = ''
    elif expected_cksum == cksum:
        verified = '(verified)'
    else:
        verified = '(WRONG!! ' + expected_cksum.hex() + ')'
    prefix = '  '
    print(prefix + 'HEADER')
    print(prefix + '-' * 56)
    prefix *= 2
    print('{}{:32} magic'.format(prefix, magic.hex()))
    print('{}{:32} command: {}'.format(prefix, command_hex.hex(), command))
    print('{}{:32} payload size: {}'.format(prefix, payload_size.hex(), psz))
    print('{}{:32} checksum {}'.format(prefix, cksum.hex(), verified))
    return command


def checksum(payload):
    """
    Method to calculate checksum of payload
    """
    return hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]

def create_message(cmd, payload = None):
    """
    Method to create header for all messages
    """
    msg = bytes()
    magic = uint32_t(0xD9B4BEF9)
    command = cmd + (12 - len(cmd)) * "\00"
    command = command.encode('utf-8')
    if payload:
        length = uint32_t(len(payload))
        checksum = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    else:
        payload = bytes()
        length = uint32_t(0)
        checksum = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    msg = magic + command + length + checksum + payload
    return msg

def create_version():
    """
    Method to create payload for version message
    """
    version = int32_t(70015)
    service = uint64_t(0)
    timestamp = int64_t(time.time())
    addr_recv_services = uint64_t(0)
    addr_recv_ip = ipv6_from_ipv4('46.229.165.138')
    addr_recv_port = uint16_t(8333) # should be big endian
    addr_my_services = uint64_t(0)
    addr_my_ip = ipv6_from_ipv4('127.0.0.1')
    addr_my_port = uint16_t(8333) # should be big endian
    nounce = uint64_t(random.getrandbits(64))
    user_agent_bytes = uint8_t(0)
    start_height = int32_t(0)
    relay = bool_t(False)
    payload = version + service + timestamp +  addr_recv_services + addr_recv_ip + addr_recv_port + addr_my_services + addr_my_ip + addr_my_port + nounce + user_agent_bytes+ start_height + relay
    return payload

def create_header_message():
    """
    Method to create payload for getheaders
    """
    version = int32_t(70015)
    hash_count = compactsize_t(1)
    header_hash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f".encode('utf-8') #hash of genesis block
    stop_hash = "0".encode('utf-8')[:32]
    payload = version + hash_count + header_hash + stop_hash
    return payload

def create_getdata(msghash):
    """
    Method to create payload for getdata
    """
    count = compactsize_t(1)
    msgtype = uint32_t(2)
    msghash1 = binascii.unhexlify(msghash)[::-1]
    inventory_entry = msgtype + msghash1
    payload = count + inventory_entry
    return payload

def send_compact():
    """
    Method to send compact message
    """
    annouce = bool_t(False)
    version = uint64_t(1)    
    payload = annouce + version
    return payload

def send_message(sendr,message):
    """
    Method to send message over socket
    """
    sendr.sendall(message)
    

def recv_message(sendr):
    """
    Method to read data from socket
    """
    print("\n************************RECEIVED MESSAGE*************************************")
    data = bytes()
    header = sendr.recv(HDR_SZ)
    data+= header
    psz = unmarshal_uint(data[16:20])
    payload = bytes()
    while(len(payload) != psz):
        payload += sendr.recv(psz - len(payload))
    data+= payload
    if data:
        return data
    else:
        return False

def check_received_message(data):
    """
    Method to identify type of message received
    Returns command
    """
    command_hex = data[4:16]
    command = str(bytearray([b for b in command_hex if b != 0]), encoding='utf-8')
    return command

def connect():
    """
    Method to connect socket
    Returns listener
    """
    sendr = socket.socket(socket.AF_INET , socket.SOCK_STREAM)
    sendr.connect(PEER_ADDR)
    print("Connecting to {}".format(PEER_ADDR))
    return sendr

def return_prev_block_hash(b):
    """
    Method to return the header hash from payload
    """
    prev_block_hash = b[4:36]
    return reverse(str(prev_block_hash.hex()))
     
def save_print_headers(payload):
    """
    Method to save the header hashes
    """
    header_hash = []
    count = unmarshal_compactsize(payload)
    offset = len(unmarshal_compactsize(payload)[0])
    blockcount = 0
    while(blockcount < count[1]):
        blockcount = blockcount +1
        prev_block_hash = return_prev_block_hash(payload[offset:offset+81])
        offset+=81
        header_hash.append(prev_block_hash)
    return header_hash

def modify_transaction(b):
    """
    Method to modify transaction
    """
    version = int32_t(1)
    tx_in_count = uint16_t(1)
    previous_output = hashlib.sha256(hashlib.sha256(b).digest()).digest()[:32] + int32_t(42)
    script_bytes = uint8_t(8)
    signature = hashlib.sha256(hashlib.sha256(b).digest()).digest()[:38]
    sequence = int64_t(4294967295)
    tx_out_count = uint16_t(1)
    value = int64_t(5000000000)
    pk_script_bytes = uint16_t(56)
    pk_script = hashlib.sha256(hashlib.sha256(b).digest()).digest()[:56]
    epoch_time = int64_t(time.time())
    return version+tx_in_count+previous_output+script_bytes+signature+sequence+tx_out_count+value+pk_script_bytes+pk_script+epoch_time
    


def communicate_with_peer(sendr):
    """
    This method communicates with peer. Sends requests and responds to messages from the peer
    """
    message = create_message("version",create_version())
    print("\n************************SENDING MESSAGE************************************")
    send_message(sendr, message)
    print_message(message)
    data = recv_message(sendr)
    while data:
        print_message(data)
        command = check_received_message(data)
        if command == 'version':
            message = create_message("verack")
            print("\n************************SENDING MESSAGE************************************")
            send_message(sendr,message)
            print_message(message)
        if command == 'verack':
            message = create_message("getheaders",create_header_message())
            print("\n************************SENDING MESSAGE************************************")
            send_message(sendr,message)
            print_message(message)
        if command == 'headers':
            payload = data[HDR_SZ:]
            print("\n************************PRINTING HEADERS***********************************")
            headerList = save_print_headers(payload)
            prev_block_hash = headerList[SUID]
            print(prev_block_hash)
            message = create_message("getdata",create_getdata(prev_block_hash)) 
            print("\n************************SENDING MESSAGE************************************")
            send_message(sendr,message)
            print_message(message)
        if command == 'block':
            print("\n")
            print("\n************************PRINTING MY BLOCK**********************************")
            print("\n")
            payload = data[HDR_SZ:]
            print_dataPayload(payload[:80])
            txn_count = unmarshal_compactsize(payload[80:])
            i = 0
            tranLen = len(txn_count[0]) + 80
            while(i<txn_count[1]):
                i=i+1
                tranLen += print_txns(payload[tranLen:])
            print("\n************************SENDING MESSAGE*************************************")
            message = create_message("block",modify_transaction(data))
            send_message(sendr,message)
            print("\n")
            print("\n************************ MODIFYING MY BLOCK TRANSACTIONS*********************")
            print("\n")
            print_message(message)
            print_txns(message[HDR_SZ:])
        data = recv_message(sendr)
    
    
         
  
if __name__ == '__main__':
    """
    Main method (Please re-excute if the socket is stuck )
    """
    sendr = connect()
    communicate_with_peer(sendr)
    
    
