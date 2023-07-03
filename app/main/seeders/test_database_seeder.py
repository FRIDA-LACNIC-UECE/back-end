import random
import re

from faker import Faker
from sqlalchemy import MetaData, Table, create_engine, insert, inspect
from sqlalchemy.orm import Session
from sqlalchemy_utils import create_database, database_exists


def fix_cpf(cpf):
    cpf = re.sub("[.-]", "", cpf)
    return cpf


def fix_rg(rg):
    rg = str(rg)
    return rg.replace("X", "0")


def insert_data(engine_db, table_name, num_of_rows, seed):
    # Creating connection with client database
    session_client_db = Session(engine_db)

    # Get columns of table
    client_columns_list = []
    insp = inspect(engine_db)
    columns_table = insp.get_columns(table_name)

    for c in columns_table:
        client_columns_list.append(str(c["name"]))

    # Create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    engine_db._metadata = MetaData(bind=engine_db)
    engine_db._metadata.reflect(engine_db)  # get columns from existing table
    engine_db._metadata.tables[table_name].columns = [
        i
        for i in engine_db._metadata.tables[table_name].columns
        if (i.name in client_columns_list)
    ]
    table_client_db = Table(table_name, engine_db._metadata)

    # Create session of database
    session_db = Session(engine_db)

    # Delete all rows of table
    session_db.query(table_client_db).delete()
    session_db.commit()

    Faker.seed(seed)
    random.seed(seed)
    faker = Faker(["pt_BR"])

    id = 1

    for _ in range(num_of_rows + 1):
        identificador = fix_cpf(faker.cpf())
        nome = faker.name()
        rg = faker.rg()
        cpf = faker.cpf()
        idade = random.randint(0, 100)
        altura = random.randint(0, 250)
        data_de_nascimento = faker.date()
        ipv4 = faker.ipv4()
        ipv6 = faker.ipv6()
        endereco = faker.address()
        email = faker.ascii_email()
        telefone = faker.cellphone_number()
        profissao = faker.job()

        stmt = insert(table_client_db).values(
            id=id,
            identificador=identificador,
            nome=nome,
            rg=rg,
            cpf=cpf,
            idade=idade,
            altura=altura,
            data_de_nascimento=data_de_nascimento,
            ipv4=ipv4,
            ipv6=ipv6,
            endereco=endereco,
            email=email,
            telefone=telefone,
            profissao=profissao,
        )

        id += 1

        session_db.execute(stmt)

    session_db.commit()


def create_test_frida_db(USER, DB_PW, HOST, DB_NAME):
    if not database_exists(
        "mysql://{}:{}@{}:3306/{}".format(USER, DB_PW, HOST, DB_NAME)
    ):
        create_database("mysql://{}:{}@{}:3306/{}".format(USER, DB_PW, HOST, DB_NAME))

    if not database_exists(
        "mysql://{}:{}@{}:3306/{}".format(USER, DB_PW, HOST, f"{DB_NAME}_backup")
    ):
        create_database(
            "mysql://{}:{}@{}:3306/{}".format(USER, DB_PW, HOST, f"{DB_NAME}_backup")
        )

    engine_db_test = create_engine(
        "mysql://{}:{}@{}:3306/{}".format(USER, DB_PW, HOST, DB_NAME)
    )
    engine_db_backup = create_engine(
        "mysql://{}:{}@{}:3306/{}".format(USER, DB_PW, HOST, f"{DB_NAME}_backup")
    )

    inspector = inspect(engine_db_test)
    list_columns_exist = list(inspector.get_table_names())

    for TABLE_NAME in ["clientes", "fornecedores"]:
        CREATE_TABLE = None

        if TABLE_NAME in list_columns_exist:
            CREATE_TABLE = False
        else:
            CREATE_TABLE = True

        if CREATE_TABLE:
            create_table = f"\
                create table {TABLE_NAME}(\
                id INT NOT NULL, \
                identificador VARCHAR(200) NOT NULL,\
                nome VARCHAR(100) NOT NULL,\
                rg VARCHAR(200) NOT NULL,\
                cpf VARCHAR(200) NOT NULL,\
                idade INT NOT NULL,\
                altura INT NOT NULL,\
                data_de_nascimento DATE,\
                ipv4 VARCHAR(20),\
                ipv6 VARCHAR(40),\
                endereco VARCHAR(200),\
                email VARCHAR(100),\
                telefone VARCHAR(50),\
                profissao VARCHAR(50),\
                PRIMARY KEY (id)\
                );"

            engine_db_test.execute(create_table)
            engine_db_backup.execute(create_table)

        # Insert fake data
        seed = 123
        insert_data(
            engine_db=engine_db_test,
            table_name=TABLE_NAME,
            num_of_rows=10000,
            seed=seed,
        )
        insert_data(
            engine_db=engine_db_backup,
            table_name=TABLE_NAME,
            num_of_rows=10000,
            seed=seed,
        )
