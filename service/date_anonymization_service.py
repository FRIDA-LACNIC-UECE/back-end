import logging
import sys
import time
from array import array
from functools import reduce
from datetime import datetime

import numpy as np
import pandas as pd
import scipy.linalg as la
from Crypto.Cipher import AES
from sqlalchemy import MetaData, Table, create_engine, select
from sqlalchemy.orm import sessionmaker

from service.database_service import get_primary_key, create_table_session


if sys.version_info < (3, 3):
    import netaddr
else:
    import ipaddress

_logger = logging.getLogger(__name__)


class AddressValueError(ValueError):
    """Exception class raised when the IP address parser (the netaddr
    module in Python < 3.3 or ipaddress module) failed.
    """
    pass


class CryptoPAn(object):
    """Anonymize IP addresses keepting prefix consitency.
    """
    def __init__(self, key):
        """Initialize a CryptoPAn() instance.
        Args:
            key: a 32 bytes object used for AES key and padding when
                 performing a block cipher operation. The first 16 bytes
                 are used for the AES key, and the latter for padding.
        Changelog: A bytes object (not string) is required for python3.
        """
        assert(len(key) == 32)
        if sys.version_info.major < 3:
            assert type(key) is str
        else:
            assert type(key) is bytes
        self._cipher = AES.new(key[:16], AES.MODE_ECB)
        self._padding = array('B')
        if sys.version_info.major == 2:
            # for Python2
            self._padding.fromstring(self._cipher.encrypt(key[16:]))
        else:
            # for Python3 (and later?)
            self._padding.frombytes(self._cipher.encrypt(key[16:]))
        self._padding_int = self._to_int(self._padding)
        self._gen_masks()

    def _gen_masks(self):
        """Generates an array of bit masks to calculate n-bits padding data.
        """
        mask128 = reduce (lambda x, y: (x << 1) | y, [1] * 128)
        self._masks = [0] * 128
        for l in range(128):
            # self._masks[0]   <- 128bits all 1
            # self._masks[127] <- 1
            self._masks[l] = mask128 >> l

    def _to_array(self, int_value, int_value_len):
        """Convert an int value to a byte array.
        """
        byte_array = array('B')
        for i in range(int_value_len):
            byte_array.insert(0, (int_value >> (i * 8)) & 0xff)
        return byte_array

    def _to_int(self, byte_array):
        """Convert a byte array to an int value.
        """
        return reduce(lambda x, y: (x << 8) | y, byte_array)

    def anonymize(self, addr):
        """Anonymize an IP address represented as a text string.
        Args:
            addr: an IP address string.
        Returns:
            An anoymized IP address string.
        """
        aaddr = None
        if sys.version_info < (3, 3):
            # for Python before 3.3
            try:
                ip = netaddr.IPNetwork(addr)
            except netaddr.AddrFormatError:
                raise AddressValueError
            aaddr = self.anonymize_bin(ip.value, ip.version)
        else:
            # for newer Python3 (and later?)
            try:
                ip = ipaddress.ip_address(addr)
            except (ValueError, ipaddress.AddressValueError) as e:
                raise AddressValueError
            aaddr = self.anonymize_bin(int(ip), ip.version)
        if ip.version == 4:
            return '%d.%d.%d.%d' % (aaddr>>24, (aaddr>>16) & 0xff,
                                    (aaddr>>8) & 0xff, aaddr & 0xff)
        else:
            return '%x:%x:%x:%x:%x:%x:%x:%x' % (aaddr>>112,
                                                (aaddr>>96) & 0xffff,
                                                (aaddr>>80) & 0xffff,
                                                (aaddr>>64) & 0xffff,
                                                (aaddr>>48) & 0xffff,
                                                (aaddr>>32) & 0xffff,
                                                (aaddr>>16) & 0xffff,
                                                aaddr & 0xffff)

    def anonymize_bin(self, addr, version):
        """Anonymize an IP address represented as an integer value.
        Args:
            addr: an IP address value.
            version: the version of the address (either 4 or 6)
        Returns:
            An anoymized IP address value.
        """
        assert(version == 4 or version == 6)
        if version == 4:
            pos_max = 32
            ext_addr = addr << 96
        else:
            pos_max = 128
            ext_addr = addr

        flip_array = []
        for pos in range(pos_max):
            prefix = ext_addr >> (128 - pos) << (128 - pos)
            padded_addr = prefix | (self._padding_int & self._masks[pos])
            if sys.version_info.major == 2:
                # for Python2
                f = self._cipher.encrypt(self._to_array(padded_addr, 16).tostring())
            else:
                # for Python3 (and later?)
                f = self._cipher.encrypt(self._to_array(padded_addr, 16).tobytes())
            flip_array.append(bytearray(f)[0] >> 7)
        result = reduce(lambda x, y: (x << 1) | y, flip_array)

        return addr ^ result


def anonymize_date(date):
    # Convert from date to string
    date = str(date)
    
    # Split day (year-month-day) by '-'
    split_date = date.split("-")
    
    # Get day and Fix day if it needs
    day = split_date[2]
    
    if day[0] == '0':
        day = day[1]
    
    # Get month and fix month if it needs
    month = split_date[1]
    
    if month[0] == '0':
        month = month[1]

    # Get year and fix year part 2 if it needs
    year = split_date[0]

    year_part1 = year[0:2]
    year_part2 = year[2:]

    if year_part2[0] == '0':
        year_part2 = year_part2[1]

    # Convert date to ip
    date_as_ip = f"{day}.{month}.{year_part1}.{year_part2}"

    # Run anonymization ip
    cp = CryptoPAn(''.join([chr(x) for x in range(0, 32)]).encode())
    anonymized_ip =  cp.anonymize(str(date_as_ip))

    # Split ip by '.'
    split_ip = anonymized_ip.split('.')

    # Get day
    day = split_ip[0]
    day = str((int(day) % 30) + 1)

    if len(day) == 1:
        day = '0' + day

    day = int(day)

    # Get month
    month = split_ip[1]
    month = str((int(month) % 12) + 1)

    if len(month) == 1:
        month = '0' + month

    month = int(month)

    # Get year
    year_part1 = split_ip[2]
    year_part2 = split_ip[3]

    year_part1 = str((int(year_part1) % 99) + 1)
    year_part2 = str((int(year_part2) % 99) + 1)

    if len(year_part1) == 1:
        year_part1 = year_part1 + '0'

    if len(year_part2) == 1:
        year_part2 = '0' + year_part2

    year = year_part1 + year_part2
    year = int(year)

    # Error with 30 and 29 on February
    if month == 2 and day >= 29:
        day = 28

    # Convert from string to datetime
    datetime_str = f"{month}-{day}-{year}"
    datetime_object = datetime.strptime(datetime_str, '%m-%d-%Y')

    return datetime_object


def anonymization_data(dataframe):
    # Anonymize dataframe
    dataframe = dataframe.applymap(anonymize_date)
    
    return dataframe

  
def anonymization_database_rows(src_client_db_path, table_name, columns_to_anonymization, rows_to_anonymization):

    #Get primary key of Client Database
    primary_key = get_primary_key(src_client_db_path, table_name)

    # Get primary key of Client Database in columns_to_anonymization only query
    columns_to_anonymization.insert(0, primary_key)

    # Create table object of Client Database and 
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, 
        table_name=table_name, 
        columns_list=columns_to_anonymization
    )
    
    # Transform rows database to dataframe
    dataframe_to_anonymization = pd.DataFrame(
        data=rows_to_anonymization
    )
    dataframe_to_anonymization = dataframe_to_anonymization[columns_to_anonymization]
   
    # Remove primary key of columns_to_anonymization list
    # but save elements in save_primary_key_elements
    save_primary_key_elements = dataframe_to_anonymization[primary_key]
    dataframe_to_anonymization = dataframe_to_anonymization.drop(primary_key, axis=1)
    columns_to_anonymization.remove(primary_key)

    # Run anonymization
    anonymization_dataframe = dataframe_to_anonymization[columns_to_anonymization]
    anonymization_dataframe = anonymization_data(anonymization_dataframe)
    anonymization_dataframe = pd.DataFrame(
        data=anonymization_dataframe, 
        columns=columns_to_anonymization
    )
    
    # Reorganize primary key elements
    anonymization_dataframe[f"{primary_key}"] = save_primary_key_elements

    # Get anonymized data to update
    dictionary_anonymized_data = anonymization_dataframe.to_dict(orient='records')

    # Update data
    for row_anonymized in dictionary_anonymized_data:
        session_client_db.query(table_client_db).\
        filter(table_client_db.c[0] == row_anonymized[f'{primary_key}']).\
        update(row_anonymized)

    session_client_db.commit()
    session_client_db.close()


def anonymization_database(src_client_db_path, table_name, columns_to_anonymization):

    #Get primary key of Client Database
    primary_key = get_primary_key(src_client_db_path, table_name)

    # Get primary key of Client Database in columns_to_anonymization only query
    columns_to_anonymization.insert(0, primary_key)

    # Create table object of Client Database and 
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, 
        table_name=table_name,
        columns_list=columns_to_anonymization
    )
        
    # Get data to dataframe
    from_dataframe = session_client_db.query(table_client_db).all()
    
    # Transform rows database to dataframe
    dataframe_to_anonymization = pd.DataFrame(
        data=from_dataframe,
        columns=columns_to_anonymization
    )
    dataframe_to_anonymization = dataframe_to_anonymization[columns_to_anonymization]

    # Remove primary key of columns_to_anonymization list
    # but save elements in save_primary_key_elements
    save_primary_key_elements = dataframe_to_anonymization[primary_key]
    dataframe_to_anonymization = dataframe_to_anonymization.drop(primary_key, axis=1)
    columns_to_anonymization.remove(primary_key)

    # Run anonymization
    anonymization_dataframe = dataframe_to_anonymization[columns_to_anonymization]
    print(anonymization_dataframe)
    anonymization_dataframe = anonymization_data(anonymization_dataframe)
    print(anonymization_dataframe)
    
    # Reorganize primary key elements
    anonymization_dataframe[f"{primary_key}"] = save_primary_key_elements

    # Get anonymized data to update
    dictionary_anonymized_data = anonymization_dataframe.to_dict(orient='records')

    # Update data
    for row_anonymized in dictionary_anonymized_data:
        session_client_db.query(table_client_db).\
        filter(table_client_db.c[0] == row_anonymized[f'{primary_key}']).\
        update(row_anonymized)

    # Commit changes
    session_client_db.commit()
    session_client_db.close()


if __name__ == '__main__':

    src_client_db_path = "mysql://root:Dd16012018@localhost:3306/test_db"
    table_name = 'nivel1'
    columns_to_anonymization = ["data_de_nascimento"]

    # Get primary key of Client Database
    primary_key = get_primary_key(src_client_db_path, table_name)

    # Get primary key of Client Database in columns_to_anonymization only query
    #columns_to_anonymization.insert(0, primary_key)

    # Create table object of Client Database and 
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_client_db_path, table_name
    )

    # Get data in database
    rows_to_anonymization = session_client_db.query(table_client_db).all()

    anonymization_database(
        src_client_db_path,
        table_name,
        columns_to_anonymization
    )