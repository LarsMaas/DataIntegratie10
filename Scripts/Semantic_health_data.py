import psycopg2
import re


def get_csv():
    """
    Get the csv file containing the health data of a person.
    :return:
    """
    health_data = {}
    keys = False
    with open('./../temp/health_data.csv') as file:
        for line in file.readlines():
            line = line.strip().split(',')
            if keys:
                key = line
                keys = False
            elif line[0] != '':
                if len(line) < 2:
                    keys = True
                else:
                    for i in range(len(key)):
                        if health_data.get(key[i]) is not None:
                            if line[i] not in health_data[key[i]]:
                                health_data[key[i]].append(line[i])
                        else:
                            health_data[key[i]] = [line[i]]

    return health_data


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
    output = {}
    domains = []
    data_types = {
        'Gender': 'Sex',
        'Race': 'Ethnicity',
        'Condition': 'Conditions or Symptom'
    }

    # Replace the dict-values in "data" with the dict-keys in "data".
    for domain, value in data_types.items():
        try:
            data[domain] = data.pop(value)
            domains.append(domain)
        except KeyError:
            pass

    # Iterates over the domains.
    for domain in domains:
        value = data[domain]
        print(value)
        # Non-list values
        if len(value) == 1:

            # Enhances the query if it has a space or bracket.
            if " " in value[0] or "(" in value[0]:
                query = enhance_query(value[0])
            else:
                query = value[0]

            # Result of the executed query appended to output.
            output[domain] = do_query(cursor, domain, query)

        # List values
        else:
            results = []

            # Iterates over the values in the list.
            for query in value:

                # Enhances the query if it has a space or bracket.
                if " " in value[0] or "(" in value[0]:
                    query = enhance_query(query)
                else:
                    pass

                # Result of the executed query appended to results.
                results.append(do_query(cursor, domain, query))

            # List of results appended to output.
            output[domain] = results

    return output


def enhance_query(query):
    """
    Text between brackets will be deleted and spaces will be replaced by
    the wildcard "%".
    :param query: the query that will be enhanced.
    :return:
    """
    query = re.sub(r'(\().*(\))', '', query).strip()
    query = query.replace(" ", "%")

    return query


def do_query(cursor, domain, query):
    """
    Query the database.
    :param cursor: necessary for querying the database.
    :param domain: the domain that will be searched in.
    :param query: the final query that will be used.
    :return:
    """
    # The query without any wildcards.
    full_query = query.replace("%", " ")

    # Searches for the query and orders the results based on a complete
    # match or whether the result starts with or ends with the query.
    cursor.execute("SELECT concept_id "
                   "FROM j3_g10.concept "
                   f"WHERE concept_name ILIKE '%{query}%' "
                   f"AND concept.domain_id LIKE '{domain}' "
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
        result = 0  # Geen gevonden race

    return result


def main():
    cursor = connect_to_db()
    health_data = get_csv()
    output = prep_query(cursor, health_data)
    print(output)


if __name__ == '__main__':
    main()
