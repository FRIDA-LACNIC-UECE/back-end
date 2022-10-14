import pandas as pd
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import sessionmaker
import anonypy


def anonymization_database(src_db_client_path, src_table, columns_to_anonymization):

    # Add id column
    columns_to_anonymization.insert(0, "id")

    # create engine, reflect existing columns, and create table object for oldTable
    # change this for your source database
    srcEngineClient = create_engine(src_db_client_path)
    SourceSessionClient = sessionmaker(srcEngineClient)
    srcEngineClient._metadata = MetaData(bind=srcEngineClient)
    srcEngineClient._metadata.reflect(srcEngineClient)  # get columns from existing table
    srcEngineClient._metadata.tables[src_table].columns = [
        i for i in srcEngineClient._metadata.tables[src_table].columns if (i.name in columns_to_anonymization)]
    srcTableClient = Table(src_table, srcEngineClient._metadata)
    sourceSessionClient = SourceSessionClient()
    
    # Get data in database
    query_client = sourceSessionClient.query(srcTableClient).all()
    result = pd.DataFrame(data=query_client, columns=columns_to_anonymization)
    print(result.head(20))

    # Remove id
    save_id = result['id']
    result = result.drop('id', axis=1)
    columns_to_anonymization.remove('id')

    # Convert columns_to_anonymization to category
    for column in columns_to_anonymization:
        result[column] = result[column].astype("category")

    # Run anonymization
    anonymization_dataframe = pd.DataFrame()

    for column in columns_to_anonymization:

        from_anonymization_dataframe = []

        #eatures_names = columns_to_anonymization.copy()
        #features_names.remove(str(column))

        p = anonypy.Preserver(result, [str(column)], str(column))
        rows = p.anonymize_t_closeness(k=3, p=0.2)
        dfn = pd.DataFrame(rows)
        print(dfn)

        for row in range(0, len(dfn)):
            from_anonymization_dataframe = from_anonymization_dataframe + [dfn[str(column)][row]] * dfn["count"][row]

        anonymization_dataframe[str(column)] = from_anonymization_dataframe
        
    #print(anonymization_dataframe.head(20))
    
    # Reorganize id
    anonymization_dataframe.index = save_id
    print(anonymization_dataframe)
    dictionary_update_data = anonymization_dataframe.to_dict(orient='records')

    # Get first id of client database
    first_line = sourceSessionClient.query(srcTableClient).first()
    id = first_line.id

    sourceSessionClient.commit()
    sourceSessionClient.close()

    # Update data anonymization
    for row_update in dictionary_update_data:
        #print(row_update)
        sourceSessionClient.query(srcTableClient).\
        filter(srcTableClient.c[0] == id).\
        update(row_update)
        id += 1

    sourceSessionClient.commit()
    
    
if __name__ == "__main__":
    anonymization_database(
        "mysql://root:Dd16012018@localhost:3306/ficticio_database",
        "nivel1",
        ["nome", "profissao"]
    )