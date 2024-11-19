import pandas as pd
import pymysql
import datetime
from pathlib import Path
from dotenv import dotenv_values
from sqlalchemy import create_engine, Column, Integer, BigInteger, String, Text, Date, Float, exc
from sqlalchemy.orm import declarative_base
import pdb

Base = declarative_base()

class Vocabulary(Base):
    __tablename__ = 'vocabulary'
    vocabulary_id = Column("vocabulary_id", String(20), primary_key=True, autoincrement=False)
    vocabulary_name = Column("vocabulary_name", String(255))
    vocabulary_reference = Column("vocabulary_reference", String(255))
    vocabulary_version = Column("vocabulary_version", String(255))
    vocabulary_concept_id = Column("vocabulary_concept_id", BigInteger, index=True)

class Concept(Base):
    __tablename__ = 'concept'
    concept_id = Column("concept_id", BigInteger, primary_key=True, )
    concept_name = Column("concept_name", String(255))
    concept_level = Column("concept_level", Integer)
    domain_id = Column("domain_id", String(20), index=True)
    concept_class_id = Column("concept_class_id", String(20), nullable=False)
    vocabulary_id = Column("vocabulary_id", String(20), nullable=True, default='Invalid', index=True)
    standard_concept = Column("standard_concept", String(1))
    concept_code = Column("concept_code", String(50), nullable=False)
    valid_start_date = Column("valid_start_date", Date, nullable=False)
    valid_end_date = Column("valid_end_date", Date, nullable=False)
    invalid_reason = Column("invalid_reason", String(1))

class ConceptAncestor(Base):
    __tablename__ = 'concept_ancestor'   
    ancestor_concept_id = Column("ancestor_concept_id", BigInteger, nullable=False, primary_key=True, index=True)
    descendant_concept_id = Column("descendant_concept_id", BigInteger, nullable=False, primary_key=True, index=True)
    min_levels_of_separation = Column("min_levels_of_separation", BigInteger, nullable=False)
    max_levels_of_separation = Column("max_levels_of_separation", BigInteger, nullable=False)

class ConceptClass(Base):
    __tablename__ = 'concept_class'
    #id = Column("id", BigInteger, primary_key=True, autoincrement=True)
    concept_class_id = Column("concept_class_id", String(20), primary_key=True, autoincrement=False)
    concept_class_name = Column("concept_class_name", String(255), nullable=False)
    concept_class_concept_id = Column("concept_class_concept_id", BigInteger, nullable=False)

class ConceptSynonym(Base):
    __tablename__ = 'concept_synonym'
    id = Column("id", BigInteger, primary_key=True, autoincrement=True)
    concept_id = Column("concept_id", BigInteger, index=True)
    concept_synonym_name = Column("concept_synonym_name", Text, nullable=False)
    language_concept_id = Column("language_concept_id", BigInteger, nullable=False)

class Domain(Base):
    __tablename__ = 'domain'   
    #id = Column("id", BigInteger, primary_key=True, autoincrement=True)
    domain_id = Column("domain_id", String(20), nullable=False, primary_key=True, autoincrement=False)
    domain_name = Column("domain_name", String(255), nullable=False)
    domain_concept_id = Column("domain_concept_id", BigInteger, nullable=False)

class DrugStrength(Base):
    __tablename__ = 'drug_strength'   
    #id = Column("id", BigInteger, primary_key=True, autoincrement=True)
    drug_concept_id = Column("drug_concept_id", BigInteger, nullable=False, primary_key=True)
    ingredient_concept_id = Column("ingredient_concept_id", BigInteger, primary_key=True)
    amount_value = Column("amount_value", Float, nullable=True)
    amount_unit_concept_id = Column("amount_unit_concept_id", BigInteger)
    numerator_value = Column("numerator_value", Float, nullable=True)
    numerator_unit_concept_id = Column("numerator_unit_concept_id", BigInteger)
    denominator_value = Column("denominator_value", Float, nullable=True)
    denominator_unit_concept_id = Column("denominator_unit_concept_id", BigInteger)
    box_size = Column("box_size", Integer)
    valid_start_date = Column("valid_start_date", Date, primary_key=True)
    valid_end_date = Column("valid_end_date", Date, primary_key=True)
    invalid_reason = Column("invalid_reason", String(1))

class Relationship(Base):
    __tablename__ = 'relationship'   
    relationship_id = Column("relationship_id", String(20), nullable=False, primary_key=True)
    relationship_name = Column("relationship_name", String(255), nullable=False)
    is_hierarchical = Column("is_hierarchical", String(1), nullable=False)
    defines_ancestry = Column("defines_ancestry", String(1), nullable=False)
    reverse_relationship_id = Column("reverse_relationship_id", String(20), nullable=False)
    relationship_concept_id = Column("relationship_concept_id", BigInteger, nullable=False)

class ConceptRelationship(Base):
    __tablename__ = 'concept_relationship'   
    #id = Column("id", BigInteger, primary_key=True, autoincrement=True)
    concept_id_1 = Column("concept_id_1", BigInteger, primary_key=True)
    concept_id_2 = Column("concept_id_2", BigInteger, primary_key=True)
    relationship_id = Column("relationship_id", String(20), primary_key=True)
    valid_start_date = Column("valid_start_date", Date, nullable=False)
    valid_end_date = Column("valid_end_date", Date, nullable=False)
    invalid_reason = Column("invalid_reason", String(1))

concept_view_create_statement = '''
CREATE OR REPLACE VIEW concepts_view AS

  SELECT c.concept_id,
         c.concept_name,
         c.domain_id as domain,
         c.vocabulary_id as vocabulary,
         c.concept_class_id as concept_class,
         c.concept_code,
         c.valid_start_date,
         c.valid_end_date,
         CASE c.invalid_reason WHEN 'U' THEN 'Invalid'
                               WHEN 'D' THEN 'Invalid'
                               ELSE 'Valid'
         END AS invalid_reason,

         CASE c.standard_concept WHEN 'C' THEN 'Classification'
                                 WHEN 'S' THEN 'Standard'
                                 ELSE 'Non-standard'
         END AS standard_concept,

         GROUP_CONCAT(concept_synonym_name, ' ') AS concept_synonym_name
         #string_agg(concept_synonym_name, ' ') AS concept_synonym_name

  FROM concept c
  LEFT JOIN concept_synonym cs on cs.concept_id = c.concept_id
  GROUP BY c.concept_id,
    c.concept_name,
    c.domain_id,
    c.vocabulary_id,
    c.concept_class_id,
    c.concept_code,
    c.valid_start_date,
    c.valid_end_date,
    c.invalid_reason,
    c.standard_concept
;
#CREATE INDEX concepts_view_concept_id_ind ON concepts_view (concept_id);
'''

concept_relationships_views_statement = '''
CREATE OR REPLACE VIEW concept_relationships_view AS
  SELECT
    cr.relationship_id  AS relationship_id,
    r.relationship_name AS relationship_name,

    sc.concept_id       AS source_concept_id,
    sc.standard_concept AS source_standard_concept,

    tc.concept_id       AS target_concept_id,
    tc.concept_name     AS target_concept_name,
    tc.vocabulary_id    AS target_concept_vocabulary_id
  FROM concept_relationship cr
    JOIN concept sc ON sc.concept_id = cr.concept_id_1
    JOIN concept tc ON tc.concept_id = cr.concept_id_2
    JOIN relationship r ON r.relationship_id = cr.relationship_id
  WHERE CURRENT_DATE BETWEEN cr.valid_start_date AND cr.valid_end_date;
'''

compound_concept_index_for_view = '''
   CREATE INDEX idx_concept_grouping ON concept (
    concept_id,
    concept_name,
    domain_id,
    vocabulary_id,
    concept_class_id,
    concept_code,
    valid_start_date,
    valid_end_date,
    invalid_reason,
    standard_concept
);
'''

def run_create_table():
    engine = get_engine()

    Base.metadata.create_all(engine)
    conn = engine.connect()
    cursor = conn.connection.cursor()
        
    if column_not_existing('concept_class', 'id', cursor=cursor):
        cursor.execute('alter table concept_class drop column id')
    
    if column_not_existing('concept_synonym', 'id', cursor=cursor):
        cursor.execute('alter table concept_synonym drop column id')
    
    if column_not_existing('domain', 'id', cursor=cursor):
        cursor.execute('alter table domain drop column id')

    if column_not_existing('drug_strength', 'id', cursor=cursor):
        cursor.execute('alter table drug_strength drop column id')

    cursor.execute(concept_view_create_statement)
    cursor.execute(concept_relationships_views_statement)
    cursor.execute(compound_concept_index_for_view)
    cursor.close()
    conn.close()
    return True

def column_not_existing(table_name, column_name, cursor):
    cursor.execute(f"select '{column_name}' from information_schema.columns where table_schema in (select schema()) and table_name='{table_name}'")
    return cursor.rowcount == 0


def get_engine():
    connection_details = get_connection_details()

    if connection_details.get('password'):
        engine_str = f"mysql+pymysql://{connection_details['user']}:{connection_details['password']}@{connection_details['host']}:{connection_details['port']}/{connection_details['database']}"
    else:
        engine_str = f"mysql+pymysql://{connection_details['user']}:@{connection_details['host']}:{connection_details['port']}/{connection_details['database']}"

    engine = create_engine(engine_str)
    return engine

def get_connection_details():
    env = dotenv_values('.env-mysql')

    # Retrieve environment variables
    return {
        "host": env['SERVER'],
        "port": env['PORT'],
        "user": env['USERNAME'],
        "password": env.get('PASSWORD'),
        "database": env['DATABASE']
    }

def get_mysql_connection():
    connection_details = get_connection_details()
    if connection_details.get("password"):
        conn = pymysql.connect(
            host=connection_details["host"],
            user=connection_details["user"],
            password=connection_details["password"],
            database=connection_details["database"],
            port=int(connection_details["port"]),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
    else:
        conn = pymysql.connect(
            host=connection_details["host"],
            user=connection_details["user"],
            database=connection_details["database"],
            port=int(connection_details["port"]),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
    return conn

def check_mysql_connection():
    try:
        conn = get_mysql_connection()
        conn.close()
        return True
    except pymysql.Error as e:
        print(f"Error connecting to MySQL: {e}")
        return False

# this will address vocabularies like rxnorm which contains 'None' as vocabulary_id 
def none_to_string(value):
    if value == 'None':
        return 'None'
    else:
        return value

def process_csv(csv, cdm_schema, vocab_file_dir, chunk_size=1000000):
    print(f"Working on file {Path(vocab_file_dir) / csv}")
    start_time = datetime.datetime.now()
    print(f"Start time: {start_time}")

    file_path = Path(vocab_file_dir) / csv
    if not file_path.exists():
        print(f"File {file_path} not found. Skipping...")
        return

    total_lines = sum(1 for _ in open(file_path, 'r', encoding='utf-8'))
    print(f"Total lines: {total_lines}")
    processed_lines = 0

    try:
        engine = get_engine()
       
        conn = engine.connect()

        if cdm_schema != '':
            table_name = f"\"{cdm_schema}\".{csv.split('.')[0]}"
        else:
            table_name = f"{csv.split('.')[0]}"

        table_name = table_name.lower()
        cursor = conn.connection.cursor()

        delete_tables = env.get('DELETE_TABLES', True) if env.get('DELETE_TABLES', True) != '' else None
        if delete_tables:
            cursor.execute(f"TRUNCATE TABLE {table_name};")

        # meta = MetaData()
        # meta.reflect(bind=engine)
        # datatable = meta.tables[table_name.lower()]
        # print([str(c.type) for c in datatable.columns])

        print(f'table_name: {table_name}')

        read_data_types = table_data_types(table_name.lower(), 'reada')
        read_data_types = None
        if read_data_types != None:
            # print(f"using data type: {read_data_types}")
            df = pd.read_csv(file_path, encoding='utf-8', delimiter='\t', dtype=read_data_types, converters={'vocabulary_id': none_to_string})   
        else:
            df = pd.read_csv(file_path, encoding='utf-8', delimiter='\t', converters={'vocabulary_id': none_to_string})


        write_data_types = table_data_types(table_name.lower(), 'writea')
        if write_data_types != None:
            # print(f"using data type: {write_data_types}")
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False, chunksize=chunk_size)        
        else:
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False, chunksize=chunk_size)

        cursor.close()
        conn.close()

        end_time = datetime.datetime.now()
        elapsed_time = end_time - start_time
        print(f"End time: {end_time}")
        print(f"Elapsed time: {elapsed_time}")
        print(f"Finished processing {csv}")
    except exc.DataError as sqle:
            pdb.set_trace()
    except pymysql.Error as e:
        pdb.set_trace()
        print(f"Database error while processing {csv}: {e}")
    except Exception as e:
        print(f"Error processing {csv}. Error: {e}")
        raise e

def load_vocab_from_csv(cdm_schema, vocab_file_dir):
    csv_list = [
        "concept.csv",
        "vocabulary.csv",
        "concept_ancestor.csv",
        "concept_relationship.csv",
        "relationship.csv",
        "concept_synonym.csv",
        "domain.csv",
        "concept_class.csv",
        "drug_strength.csv"
    ]

    file_list = [f.name for f in Path(vocab_file_dir).glob('*') if f.name.lower() in csv_list]

    for csv in file_list:
        process_csv(csv, cdm_schema, vocab_file_dir)

def table_data_types(name, kind):
    # type_dict = {   
    #         "vocabulary": {
    #             "vocabulary_id": {"read": int, "write": BigInteger},
    #             "vocabulary_name": {"read": str, "write": String},
    #             "vocabulary_reference": {"read": str, "write": String},
    #             "vocabulary_version": {"read": str, "write": String},
    #             "vocabulary_concept_id": {"read": int, "write": BigInteger},
    #     },
    # }
    # result = {}
    # column_types = type_dict.get(name, {})
    #
    # for column, column_info in column_types.items():
    #     result[column] = column_info.get(kind)
    #
    # #print(f'result for name: {name}, kind: {kind}: {result}')
    # return None if None in result.values() else result
    return None

if __name__ == '__main__':

    env = dotenv_values('.env-mysql')

    cdm_schema = env.get('CDM_SCHEMA', None) if env.get('CDM_SCHEMA', None) != '' else None
    vocab_file_dir = env['VOCAB_FILE_DIR']

    print('checking connection to mysql')
    if not check_mysql_connection():
        print("Exiting...")
        exit()
    print('connection OK')

    if env.get('CREATE_TABLES', False) in ['True', 'true', True, '1', 1]:
        run_create_table()

    load_vocab_from_csv(cdm_schema, vocab_file_dir)