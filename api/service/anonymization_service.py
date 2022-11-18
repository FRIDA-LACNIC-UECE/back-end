from model.anonymization_type_model import AnonymizationType

from service.anonymization_types import (
    anonypy_anonymization_service, date_anonymization_service, 
    ip_anonymization_service, ppcbtf_anonymization_service,
    ppcbti_anonymization_service
)


def anonymization_database_rows(src_client_db_path, lists_columns_anonymizations, rows_to_anonymization):

    # Get each anonymization types
    for anonymization in lists_columns_anonymizations:
        
        # Get database path by id
        anonymization_type_name = AnonymizationType.query.filter_by(id=anonymization['id_anonymization_type']).first().name
    
        # Run anonymization
        if anonymization_type_name == "ppcbtf":
            ppcbtf_anonymization_service.anonymization_database_rows(
                src_client_db_path, 
                table_name=anonymization['table'],
                columns_to_anonymization=anonymization['columns'],
                rows_to_anonymization=rows_to_anonymization
            )
            print("\n\n ppcbtf anonimizando\n\n")

        elif anonymization_type_name == "ppcbti":
            ppcbti_anonymization_service.anonymization_database_rows(
                src_client_db_path, 
                table_name=anonymization['table'],
                columns_to_anonymization=anonymization['columns'],
                rows_to_anonymization=rows_to_anonymization
            )
            print("\n\n ppcbti anonimizando\n\n")

        elif anonymization_type_name == "anonypy":
            anonypy_anonymization_service.anonymization_database_rows(
                src_client_db_path, 
                table_name=anonymization['table'],
                columns_to_anonymization=anonymization['columns'],
                rows_to_anonymization=rows_to_anonymization
            )
            print("\n\n nonypy anonimizando\n\n")
        
        elif anonymization_type_name == "ip_anonymizer":
            ip_anonymization_service.anonymization_database_rows(
                src_client_db_path, 
                table_name=anonymization['table'],
                columns_to_anonymization=anonymization['columns'],
                rows_to_anonymization=rows_to_anonymization
            )
            print("\n\n ip_anonymizer anonimizando\n\n")
        
        elif anonymization_type_name == "date_anonymizer":
            date_anonymization_service.anonymization_database_rows(
                src_client_db_path, 
                table_name=anonymization['table'],
                columns_to_anonymization=anonymization['columns'],
                rows_to_anonymization=rows_to_anonymization
            )
            print("\n\n date_anonymizer anonimizando\n\n")
        
        else:
            return 406 #Not Acceptable

    return 200 #OK


def anonimization_database(src_client_db_path, lists_columns_anonymizations):

    # Get each anonymization types
    for anonymization in lists_columns_anonymizations:
        
        # Get database path by id
        anonymization_type_name = AnonymizationType.query.filter_by(id=anonymization['id_anonymization_type']).first().name
    
        # Run anonymization
        if anonymization_type_name == "ppcbtf":
            ppcbtf_anonymization_service.anonymization_database(
                src_client_db_path, 
                table_name=anonymization['table'],
                columns_to_anonymization=anonymization['columns']
            )
            print("\n\n ppcbtf anonimizando\n\n")

        elif anonymization_type_name == "ppcbti":
            ppcbti_anonymization_service.anonymization_database(
                src_client_db_path, 
                table_name=anonymization['table'],
                columns_to_anonymization=anonymization['columns']
            )
            print("\n\n ppcbti anonimizando\n\n")

        elif anonymization_type_name == "anonypy":
            anonypy_anonymization_service.anonymization_database(
                src_client_db_path, 
                table_name=anonymization['table'],
                columns_to_anonymization=anonymization['columns']
            )
            print("\n\n anonypy anonimizando\n\n")
        
        elif anonymization_type_name == "ip_anonymizer":
            ip_anonymization_service.anonymization_database(
                src_client_db_path, 
                table_name=anonymization['table'],
                columns_to_anonymization=anonymization['columns']
            )
            print("\n\n ip_anonymizer anonimizando\n\n")

        elif anonymization_type_name == "date_anonymizer":
            date_anonymization_service.anonymization_database(
                src_client_db_path, 
                table_name=anonymization['table'],
                columns_to_anonymization=anonymization['columns']
            )
            print("\n\n date_anonymizer anonimizando\n\n")
        
        else:
            return 406 #Not Acceptable

    return 200 #OK