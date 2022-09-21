import datetime

import jwt
import pandas as pd
from flask import jsonify, request
from flask_migrate import Migrate
from sqlalchemy_utils import database_exists
from sqlalchemy import create_engine, exc

from controller import app, db
from service.authenticate import jwt_required
from model.models import (AppMeta, Client, Client2, MainDB, User, Database, ValidDatabase, appmeta_share_schema,
                          appmetas_share_schema, maindb_share_schema,
                          maindbs_share_schema, user_share_schema,
                          users_share_schema, clients_share_schema, clients2_share_schema,
                          database_share_schema, databases_share_schema,
                          valid_database_share_schema, valid_databases_share_schema)

from service import SSE, service, RSA


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

    RSA.encrypt_database(src_db_client_path, src_db_dest_path, src_table, columns_list, size_batch)

    return jsonify({
        'message': 'Banco de dados encriptografado com sucesso!'
    })


@ app.route('/include_column_hash', methods=['GET'])
def include_column_hash():

    src_db_cloud = request.json['src_db_cloud']
    src_db_cloud_path = "{}://{}:{}@{}:{}/{}".format(src_db_cloud['type'], src_db_cloud['user'],
                                               src_db_cloud['password'], src_db_cloud['ip'], src_db_cloud['port'], src_db_cloud['name'])

    src_table = src_db_cloud['table']

    src_db_user = request.json['src_db_user']
    src_db_user_path = "{}://{}:{}@{}:{}/{}".format(src_db_user['type'], src_db_user['user'],
                                               src_db_user['password'], src_db_user['ip'], src_db_user['port'], src_db_user['name'])

    SSE.include_column_hash(src_db_cloud_path, src_db_user_path, src_table)

    return jsonify({
        'message': 'Incluido coluna hash com sucesso!'
    })


@ app.route('/line_by_hash', methods=['GET'])
def line_by_hash():

    src_db_cloud = request.json['src_db_cloud']
    src_db_cloud_path = "{}://{}:{}@{}:{}/{}".format(src_db_cloud['type'], src_db_cloud['user'],
                                               src_db_cloud['password'], src_db_cloud['ip'], src_db_cloud['port'], src_db_cloud['name'])
    
    table_name = src_db_cloud["table"]

    hash = request.json['line_hash']

    row_found = SSE.line_by_hash(src_db_cloud_path, table_name, hash)

    return jsonify({
        'message': 'Pesquisa por hash concluída com sucesso!',
        'row_found': row_found
    })


@ app.route('/line_by_id', methods=['GET'])
def line_by_id():

    src_db_cloud = request.json['src_db_cloud']
    src_db_cloud_path = "{}://{}:{}@{}:{}/{}".format(src_db_cloud['type'], src_db_cloud['user'],
                                               src_db_cloud['password'], src_db_cloud['ip'], src_db_cloud['port'], src_db_cloud['name'])
    
    table_name = src_db_cloud["table"]

    id = request.json['id']

    row_found = SSE.line_by_id(src_db_cloud_path, table_name, id)

    return jsonify({
        'message': 'Pesquisa por hash concluída com sucesso!',
        'row_found': row_found
    })


@ app.route('/register', methods=['POST'])
def register():
    name = request.json['name']
    email = request.json['email']
    pwd = request.json['password']
    is_admin = False

    user = User.query.filter_by(email=email).first()

    if user:
        return jsonify({
            'error': 'User already registered, please try again!'
        }), 409

    user = User(name, email, pwd, is_admin)
    db.session.add(user)
    db.session.commit()

    return jsonify({
        'message': 'User registered successfully!'
    })


@ app.route('/login', methods=['POST'])
def login():
    email = request.json['email']
    pwd = request.json['password']

    user = User.query.filter_by(email=email).first()

    if not user or not user.verify_password(pwd):
        return jsonify({
            "error": "Incorrect data, please try again!"
        }), 403

    payload = {
        "id": user.id,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=60)
    }

    token = jwt.encode(payload, app.config['SECRET_KEY'])

    return jsonify({
        'message': 'Logged successfully!',
        'is_admin': user.is_admin,
        'token': token
    })


@ app.route('/getUser', methods=['GET'])
@ jwt_required
def getUser(current_user):
    return jsonify(user_share_schema.dump(current_user))


@ app.route('/getUsers', methods=['GET'])
@ jwt_required
def getUsers(current_user):
    result = users_share_schema.dump(
        User.query.all()
    )
    return jsonify(result)


@ app.route('/getValidDatabases', methods=['GET'])
@ jwt_required
def getValidDatabases(current_user):

    result = valid_databases_share_schema.dump(
        ValidDatabase.query.all()
    )
    return jsonify(result)


@ app.route('/getDatabases', methods=['GET'])
@ jwt_required
def getDatabases(current_user):

    id = current_user.id

    result = databases_share_schema.dump(
        Database.query.filter_by(id_user=id).all()
    )
    result2 = valid_databases_share_schema.dump(
        ValidDatabase.query.all()
    )
    for database in result:
        for type in result2:
            if type['id'] == database['id_db_type']:
                database['name_db_type'] = type['name']
    return jsonify(result)


@ app.route('/addDatabase', methods=['POST'])
@ jwt_required
def addDatabase(current_user):
    id_user = current_user.id
    id_db_type = request.json['id_db_type']
    name = request.json['name']
    host = request.json['host']
    user = request.json['user']
    port = request.json['port']
    pwd = request.json['password']

    database = Database(id_user, id_db_type, name, host, user, port, pwd, '')
    db.session.add(database)
    db.session.commit()

    return jsonify({
        'message': 'Database added successfully!'
    })


@ app.route('/deleteDatabase', methods=['POST'])
@ jwt_required
def deleteDatabase(current_user):

    id = request.json['id']

    database = Database.query.filter_by(id=id).first()

    if not database:
        return jsonify({
            'error': 'Database doesn\'t exist, please try again!'
        }), 409

    db.session.delete(database)
    db.session.commit()

    result = database_share_schema.dump(
        Database.query.filter_by(id=id).first()
    )

    if not result:
        return jsonify({'message': 'Database deleted successfully!'})
    else:
        return jsonify({'error': 'Could\'nt delete database, please try again!'})


@ app.route('/deleteUser', methods=['POST'])
@ jwt_required
def deleteUser(current_user):

    email = request.json['email']

    user = User.query.filter_by(email=email).first()

    if not user:
        return jsonify({
            'error': 'User does not exist, please try again!'
        }), 409

    db.session.delete(user)
    db.session.commit()

    result = user_share_schema.dump(
        User.query.filter_by(email=email).first()
    )

    if not result:
        return jsonify({'message': 'User deleted successfully!'})
    else:
        return jsonify({'error': 'Unable to delete user, please try again!'})


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
