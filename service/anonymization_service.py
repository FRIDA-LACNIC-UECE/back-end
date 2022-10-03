from model.anonymization_type_model import AnonymizationType
from service import ppcbtf_anonymization_service, ppcbti_anonymization_service


def anonimization_database(src_client_db_path, lists_columns_anonymizations):

    # Get each anonymization types
    for anonymization in lists_columns_anonymizations:
        
        # Get database path by id
        anonymization_type_name = AnonymizationType.query.filter_by(id=anonymization['id_anonymization_type']).first().name
    
        # Run anonymization
        if anonymization_type_name == "ppcbtf":
            ppcbtf_anonymization_service.anonymization_database(
                src_client_db_path, 
                src_table=anonymization['table'],
                columns_to_anonymization=anonymization['columns']
            )
            print("\n\nppcbtf anonimizando\n\n")

        elif anonymization_type_name == "ppcbti":
            ppcbti_anonymization_service.anonymization_database(
                src_client_db_path, 
                src_table=anonymization['table'],
                columns_to_anonymization=anonymization['columns']
            )
            print("\n\nppcbti anonimizando\n\n")
        
        else:
            return 406 #Not Acceptable

    return 200 #OK