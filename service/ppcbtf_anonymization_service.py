import numpy as np
import pandas as pd
import scipy.linalg as la
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import sessionmaker


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

    # Remove id
    save_id = result['id']
    print(result)
    result = result.drop('id', axis=1)
    columns_to_anonymization.remove('id')

    # Run anonymization
    result_anonymization = result[columns_to_anonymization].astype('int')
    result_anonymization = result_anonymization.to_numpy()
    result_anonymization = anonymization_data(result_anonymization)
    result[columns_to_anonymization] = pd.DataFrame(data=result_anonymization, columns=columns_to_anonymization)
    
    # Reorganize id
    result.index = save_id
    print(result)
    dictionary_update_data = result.to_dict(orient='records')
    first_line = sourceSessionClient.query(srcTableClient).first()
    id = first_line.id
    print(first_line)

    sourceSessionClient.commit()
    sourceSessionClient.close()

    # Update data anonymization
    for row_update in dictionary_update_data:
        print(row_update)
        sourceSessionClient.query(srcTableClient).\
        filter(srcTableClient.c[0] == id).\
        update(row_update)
        id += 1

    sourceSessionClient.commit()


def anonymization_data(data):
    # calculate the mean of each column
    mean = np.array(np.mean(data, axis=0).T)

    # center data
    data_centered = data - mean

    # calculate the covariance matrix
    cov_matrix = np.cov(data_centered, rowvar=False)

    # calculate the eignvalues and eignvectors
    evals, evecs = la.eigh(cov_matrix)

    # sort them
    idx = np.argsort(evals)[::-1]

    # Each columns of this matrix is an eingvector
    evecs = evecs[:, idx]
    evals = evals[idx]

    # explained variance
    variance_retained = np.cumsum(evals)/np.sum(evals)

    # calculate the transformed data
    data_transformed = np.dot(evecs.T, data_centered.T).T

    # randomize eignvectors
    new_evecs = evecs.copy().T
    for i in range(len(new_evecs)):
        np.random.shuffle(new_evecs[i])
    new_evecs = np.array(new_evecs).T

    # go back to the original dimension
    data_original_dimension = np.dot(data_transformed, new_evecs.T)
    data_original_dimension += mean

    return data_original_dimension