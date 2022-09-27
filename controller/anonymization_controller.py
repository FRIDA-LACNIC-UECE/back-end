'''@app.route('/test_anon', methods=['GET'])
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
    return jsonify(MainDB=result, AppMeta=result2)'''