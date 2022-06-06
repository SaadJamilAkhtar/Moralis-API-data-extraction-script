from time import sleep
import requests
import mysql.connector

base_url = "https://deep-index.moralis.io/api/v2/nft/transfers?chain=eth&format=decimal"

payload = {}
headers = {
    'X-API-Key': 'o5RMFeL3wZteMNoNzlSP6eoSSyzcXzyDdq1Eo1YmAUz9evLhojjZwARZrpVkL2eC'
}


def getData(url, mycursor):
    response = requests.request("GET", url, headers=headers, data=payload)
    if int(response.json()['total']) == 0:
        return False
    writeToFile(response, mycursor)
    cursor = response.json()['cursor']
    while cursor:
        response = requests.request("GET", f'{url}&cursor={cursor}', headers=headers, data=payload)
        writeToFile(response, mycursor)
        cursor = response.json()['cursor']
    return True


def writeToFile(response, mycursor):
    file = open('data.csv', 'a')
    data = response.json()['result']
    for entry in data:
        print(f'{entry["block_number"]},{entry["block_timestamp"]},{entry["block_hash"]},'
              f'{entry["transaction_hash"]},{entry["transaction_index"]},{entry["log_index"]}\n')
        file.write(f'{entry["block_number"]},{entry["block_timestamp"]},{entry["block_hash"]},'
                   f'{entry["transaction_hash"]},{entry["transaction_index"]},{entry["log_index"]}\n')
        sql = f"INSERT INTO eth (block_number, block_timestamp, block_hash, transaction_hash, transaction_index, " \
              f"log_index, value, contract_type, transaction_type, token_address, token_id, from_address, to_address, " \
              f"amount, verified, operator) VALUES ('{entry['block_number']}', '{entry['block_timestamp']}'," \
              f"'{entry['block_hash']}','{entry['transaction_hash']}', '{entry['transaction_index']}', " \
              f"'{entry['log_index']}', '{entry['value']}', '{entry['contract_type']}', '{entry['transaction_type']}'," \
              f"'{entry['token_address']}', '{entry['token_id']}', '{entry['from_address']}', '{entry['to_address']}', " \
              f"'{entry['amount']}', {entry['verified']}, '{entry['operator']}')"
        mycursor.execute(sql)
    mydb.commit()


if __name__ == '__main__':
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root1122",
        database="block"
    )
    mycursor = mydb.cursor()

    retry = 0

    initial_url = f"{base_url}&from_block=1000"
    response = requests.request("GET", initial_url, headers=headers, data=payload)
    start_block_num = response.json()['result'][0]["block_number"]
    print(start_block_num)
    start_url = f"{base_url}&from_block={start_block_num}&to_block={start_block_num}"
    getData(start_url, mycursor)
    start_block_num = int(start_block_num) + 1
    while True:
        print(start_block_num)
        start_url = f"{base_url}&from_block={start_block_num}&to_block={start_block_num}"
        if getData(start_url, mycursor):
            start_block_num = int(start_block_num) + 1
            retry = 0
        else:
            retry += 1
        if retry > 10:
            retry = 0
            start_block_num = int(start_block_num) + 1
        sleep(10)
