import datetime

import pandas as pd
from flask import jsonify, request
from flask_migrate import Migrate
from sqlalchemy_utils import database_exists
from sqlalchemy import create_engine, exc

from controller import app, db
from service.authenticate import jwt_required

from model.valid_database_model import ValidDatabase, ValidDatabaseSchema, valid_database_share_schema, valid_databases_share_schema
from model.user_model import User, UserSchema, user_share_schema, users_share_schema
from model.models import (AppMeta, Client, Client2, MainDB, appmeta_share_schema,
                          appmetas_share_schema, maindb_share_schema,
                          maindbs_share_schema, clients_share_schema, clients2_share_schema)

from service import rsa_service, service, sse_service

from controller import user_database, database_controller, valid_database_controller


Migrate(app, db)


@app.shell_context_processor
def make_shell_context():
    return dict(
        app=app,
        db=db,
        User=User
    )


@app.route('/')
def index():
    return 'Flask is running'


@app.route('/test_anon', methods=['GET'])
def test_anon():
    columns = [
        '_0',
        '_1',
        '_2',
        '_3',
        '_4',
        '_5',
        '_6',
        '_7',
        '_8',
        '_9'
    ]

    result = clients_share_schema.dump(
        Client.query.limit(1000).all()
    )

    df = pd.DataFrame(data=result, columns=columns).astype(int)
    df2 = pd.DataFrame(data=service.anonimization(df), columns=columns)

    # Create engine to connect with DB
    try:
        engine = create_engine(
            'mysql://root:Admin538*@localhost:3306/public')
    except:
        print("Can't create 'engine")

    # Standart method of Pandas to deliver data from DataFrame to PastgresQL
    try:
        with engine.begin() as connection:
            df2.to_sql('client2', con=connection,
                       index_label='id', if_exists='replace')
            print('Done, ok!')
    except:
        print('Something went wrong!')

    return jsonify(before=df.values.tolist(), after=df2.values.tolist())


@ app.route('/copy_database', methods=['GET'])
def copy_database():

    src_db = request.json['src_db']
    src_db_path = "{}://{}:{}@{}:{}/{}".format(src_db['type'], src_db['user'],
                                               src_db['password'], src_db['ip'], src_db['port'], src_db['name'])
    src_table = src_db['table']

    dest_db = request.json['dest_db']
    dest_db_path = "{}://{}:{}@{}:{}/{}".format(dest_db['type'], dest_db['user'],
                                                dest_db['password'], dest_db['ip'], dest_db['port'], dest_db['name'])
    dest_table = dest_db['table']
    dest_columns = dest_db['columns']

    service.copy_database_fc(src_db_path, dest_db_path,
                     src_table, dest_columns, dest_table)
    service.create_model(dest_db_path, dest_table, dest_columns)

    return jsonify({
        'message': 'Database copied successfully!'
    })


@ app.route('/anonimization_data2', methods=['GET'])
def anonimization_data2():

    src_db_client = request.json['src_db_client']

    src_db_client_path = "{}://{}:{}@{}:{}/{}".format(src_db_client['type'], src_db_client['user'],
                                               src_db_client['password'], src_db_client['ip'], src_db_client['port'], src_db_client['name'])

    src_table = src_db_client['table']

    columns_to_anonimization = src_db_client['columns']

    service.anonimization_data(src_db_client_path, src_table, columns_to_anonimization)

    return jsonify({
        'message': 'Dados anonimizado com sucesso!'
    })


@ app.route('/encrypt_database', methods=['GET'])
def encrypt_database():

    src_db_client = request.json['src_db_client']

    src_db_client_path = "{}://{}:{}@{}:{}/{}".format(
        src_db_client['type'], src_db_client['user'], src_db_client['password'], 
        src_db_client['ip'], src_db_client['port'], src_db_client['name']
    )

    src_table = src_db_client['table']

    columns_list = src_db_client["columns"]

    src_db_dest = request.json['src_db_cloud']

    src_db_dest_path = "{}://{}:{}@{}:{}/{}".format(
        src_db_dest['type'], src_db_dest['user'], src_db_dest['password'], 
        src_db_dest['ip'], src_db_dest['port'], src_db_dest['name']
    )

    size_batch = 1000

    rsa_service.encrypt_database(src_db_client_path, src_db_dest_path, src_table, columns_list, size_batch)

    return jsonify({
        'message': 'Banco de dados encriptografado com sucesso!'
    })


@ app.route('/protected', methods=['GET'])
@ jwt_required
def protected(current_user):
    result = appmetas_share_schema.dump(
        AppMeta.query.all()
    )
    result2 = maindbs_share_schema.dump(
        MainDB.query.all()
    )
    return jsonify(AppMeta=result, MainDB=result2)


@ app.route('/anonymize_data', methods=['GET'])
@ jwt_required
def encrypt_data(current_user):
    result = appmetas_share_schema.dump(
        AppMeta.query.all()
    )

    result2 = result

    for index, x in enumerate(result):
        exists = MainDB.query.filter_by(user_id=x['id']).update(
            {'cpf': x['cpf']}, synchronize_session="fetch")
        if(not exists):
            db.session.add(MainDB(x['id'], x['email'], x['cpf']))

        AppMeta.query.filter_by(
            id=x['id']).update({'email': result2[index]['email'], 'cpf': result2[index]['cpf']})
    db.session.commit()
    result = maindbs_share_schema.dump(
        MainDB.query.all()
    )
    result2 = appmetas_share_schema.dump(
        AppMeta.query.all()
    )
    return jsonify(MainDB=result, AppMeta=result2)


@app.route('/deanonymize_data', methods=['GET'])
@jwt_required
def deencrypt_data(current_user):
    result = maindbs_share_schema.dump(
        MainDB.query.all()
    )
    for x in result:
        exists = AppMeta.query.filter_by(id=x['user_id']).update(
            {'cpf': x['cpf'], 'email': x['email']}, synchronize_session="fetch")
        if(not exists):
            return jsonify({
                "error": "User not found!"
            }), 403
        MainDB.query.filter_by(
            user_id=x['user_id']).delete()
    db.session.commit()
    result = maindbs_share_schema.dump(
        MainDB.query.all()
    )
    result2 = appmetas_share_schema.dump(
        AppMeta.query.all()
    )
    return jsonify(MainDB=result, AppMeta=result2)


@app.route('/test_connection', methods=['POST'])
@jwt_required
def test_connection(current_user):
    try:
        db = "{}://{}:{}@{}:{}/{}".format(request.json['type'].lower(), request.json['user'],
                                          request.json['password'], request.json['host'], request.json['port'], request.json['name'])
        engine = create_engine(db)
        if database_exists(engine.url):
            return jsonify({
                'message': 'Connection Successful!'
            })
    except:
        return jsonify({
            'error': 'Cannot check if database exists!'
        }), 409


@ app.route('/getDatabase', methods=['GET'])
def getDatabase():
    result = clients_share_schema.dump(
        Client.query.limit(1000).all()
    )
    return jsonify(result)


@ app.route('/getDatabase2', methods=['GET'])
def getDatabase2():
    result = clients2_share_schema.dump(
        Client2.query.limit(1000).all()
    )
    return jsonify(result)


'''
@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('login'))
 '''

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

'''ssl_context=('ca/cert.pem', 'ca/key.pem')'''
