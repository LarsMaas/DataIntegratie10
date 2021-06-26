import psycopg2
import re


def get_snp():
    """
    Get the 10 missense/frameshift variants from chr 21 from a person
    :return:
    """
    with open('./temp/chr21_ann_10.vcf') as file:
        file.readline()
        snps = [line.split('\t') for line in file.readlines()]
    return snps


def connect_to_db():
    """
    Make a connection to the database.
    :return:
    """
    connection = psycopg2.connect(
        host='145.74.104.145',
        database='postgres',
        user='j3_g10',
        password='Blaat1234',
        port=5432
    )
    return connection.cursor()


def prep_query(cursor, data):
    """
    Prepare the variables to be added to the SQL query.
    :param cursor: necessary for querying the database.
    :param data: a dictionary of the Sex, Ethnicity and Conditions.
    :return:
    """
    output = []

    for snp in data:
        query = f'chr%21%{snp[1]}%{snp[3]}%{snp[4]}'
        output.append(do_query(cursor, query))

    return output


def do_query(cursor, query):
    """
    Query the database.
    :param cursor: necessary for querying the database.
    :param query: the final query that will be used.
    :return:
    """
    # The query without any wildcards.
    full_query = query.replace("%", " ")

    # Searches for the query and orders the results based on a complete
    # match or whether the result starts with or ends with the query.
    # Query looks like: 'chr%21%<position>%<Reference a.a>%<Actual a.a>
    cursor.execute("SELECT concept_id "
                   "FROM j3_g10.concept "
                   f"WHERE concept_name ILIKE '%{query}%' "
                   f"AND concept.concept_class_id LIKE 'Genomic Variant' "
                   "ORDER BY "
                   "CASE "
                   f"   WHEN concept_name ILIKE '{full_query}' THEN 1 "
                   f"   WHEN concept_name ILIKE '{query}%' THEN 2 "
                   f"   WHEN concept_name ILIKE '%{query}' THEN 4 "
                   "ELSE 3 "
                   "END "
                   "LIMIT 10;"
                   )

    result = cursor.fetchone()

    # The actual result of the query is selected.
    try:
        if len(result) == 1:
            result = result[0]
    except TypeError:
        pass

    if result is None:
        result = 0

    return result


def main():
    cursor = connect_to_db()
    snp_data = get_snp()
    output = prep_query(cursor, snp_data)
    return output


if __name__ == '__main__':
    main()
