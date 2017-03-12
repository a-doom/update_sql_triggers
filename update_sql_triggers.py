import pypyodbc
import os
import sys
import argparse
import re


CONNECTION_STRING = "Driver={SQL Server};Server=SERVERNAME;Database=DATABASENAME;Trusted_Connection=yes;"
TRIGGERS_PATH = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), "procedures_and_triggers")

transaction_query_start = "DECLARE @error_object_name varchar(300);\n" \
                          "BEGIN TRY;\n" \
                          "BEGIN TRANSACTION;\n\n"

transaction_query_end = "COMMIT TRANSACTION;\n" \
                        "END TRY\n" \
                        "BEGIN CATCH;\n" \
                        "\tROLLBACK TRANSACTION;\n" \
                        "\tTHROW 50000,\n" \
                        "\t@error_object_name, 1;\n" \
                        "END CATCH;\n"

select_triggers_query = \
    """
    SELECT DISTINCT
        o.name		AS [object_name],
        o.type_desc		AS [type_desc],
        m.definition	AS [object_text]
    FROM sys.sql_modules  m
    INNER JOIN sys.objects  o ON m.object_id=o.object_id
    WHERE type in ('tr', 'p', 'if','fn','tf')
        AND is_ms_shipped = 0
    """

class ObjectTypes:
    SQL_TRIGGER="SQL_TRIGGER"
    SQL_STORED_PROCEDURE="SQL_STORED_PROCEDURE"
    SQL_SCALAR_FUNCTION="SQL_SCALAR_FUNCTION"
    SQL_INLINE_TABLE_VALUED_FUNCTION="SQL_INLINE_TABLE_VALUED_FUNCTION"
    SQL_TABLE_VALUED_FUNCTION="SQL_TABLE_VALUED_FUNCTION"



class SqlObject():
    def __init__(self, object_name, type_desc, object_text):
        self.object_name = object_name
        self.type_desc = type_desc
        self.object_text = object_text
        self.stripped_text = rstrip_every_line(object_text)
        self.is_new = False

    def __eq__(self, other):
        return (isinstance(other, self.__class__)
            and self.__dict__ == other.__dict__)


def drop_if_exists_query(sql_object):
    if sql_object.type_desc == ObjectTypes.SQL_TRIGGER:
        result = "IF EXISTS (SELECT * FROM sys.objects WHERE [name] = N" \
                 "'{0}' AND [type] = 'TR') DROP TRIGGER [dbo].[{0}];".format(sql_object.object_name)
    elif sql_object.type_desc == ObjectTypes.SQL_STORED_PROCEDURE:
        result = "IF EXISTS ( SELECT * FROM   sysobjects WHERE  id = object_id(" \
                 "N'[dbo].[{0}]') and OBJECTPROPERTY(id, N'IsProcedure') = 1 ) DROP PROCEDURE [dbo].[{0}];".format(sql_object.object_name)
    elif sql_object.type_desc in (
            ObjectTypes.SQL_SCALAR_FUNCTION, ObjectTypes.SQL_INLINE_TABLE_VALUED_FUNCTION,
            ObjectTypes.SQL_TABLE_VALUED_FUNCTION):
        result = "IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id(" \
                 "N'{0}') AND xtype IN (N'FN', N'IF', N'TF')) DROP FUNCTION {0};".format(sql_object.object_name)
    else:
        raise TypeError(sql_object.type_desc)
    return result


def throw_if_not_exists_query(sql_object):
    if sql_object.type_desc == ObjectTypes.SQL_TRIGGER:
        result = "IF NOT EXISTS (SELECT * FROM sys.objects WHERE [name] = N" \
                 "'{0}' AND [type] = 'TR') THROW 70000, '{0} does not exist!', 1;".format(sql_object.object_name)
    elif sql_object.type_desc == ObjectTypes.SQL_STORED_PROCEDURE:
        result = "IF NOT EXISTS ( SELECT * FROM   sysobjects WHERE  id = object_id(" \
                 "N'[dbo].[{0}]') and OBJECTPROPERTY(id, N'IsProcedure') = 1 ) THROW 70000, '{0} does not exist!', 1;".format(sql_object.object_name)
    elif sql_object.type_desc in (
            ObjectTypes.SQL_SCALAR_FUNCTION, ObjectTypes.SQL_INLINE_TABLE_VALUED_FUNCTION,
            ObjectTypes.SQL_TABLE_VALUED_FUNCTION):
        result = "IF NOT EXISTS (SELECT * FROM sysobjects WHERE id = object_id(" \
                 "N'{0}') AND xtype IN (N'FN', N'IF', N'TF')) THROW 70000, '{0} does not exist!', 1;".format(sql_object.object_name)
    else:
        raise TypeError(sql_object.type_desc)
    return result


def find_object_type(object_text):
    if re.search(r'\bCREATE PROCEDURE\b', object_text, flags=re.IGNORECASE):
        result =ObjectTypes.SQL_STORED_PROCEDURE
    elif re.search(r'\bCREATE FUNCTION\b', object_text, flags=re.IGNORECASE):
        result = ObjectTypes.SQL_SCALAR_FUNCTION
    elif re.search(r'\bCREATE TRIGGER\b', object_text, flags=re.IGNORECASE):
        result = ObjectTypes.SQL_TRIGGER
    else:
        raise TypeError()
    return result


def format_subquery(query, name=None):
    if name is not None:
        return "SET @error_object_name = '{0}'\nEXEC sp_ExecuteSQL N'\n{1}\n';\n".format(name, query.replace("'", "''"))
    else:
        return "EXEC sp_ExecuteSQL N'\n{0}\n';\n".format(query.replace("'", "''"))


def rstrip_every_line(lines):
    result = []
    lines = lines.split("\n")
    for line in lines:
        line = line.rstrip()
        if len(line) > 0:
            result.append(line)
    return "\n".join(result)


def get_sql_objects(connection_string):
    result = {}
    with pypyodbc.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute(select_triggers_query)
            while True:
                row = cursor.fetchone()
                if not row:
                    break
                object_name = row['object_name'].lower()
                result[object_name] = SqlObject(
                    object_name=object_name,
                    type_desc=row['type_desc'],
                    object_text=row['object_text'])
    return result


def execute_sql_query(connection_string, query_string):
    # print(query_string)
    with pypyodbc.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query_string)


def get_file_objects(triggers_path):
    result = {}
    for filename in sorted(os.listdir(triggers_path)):
        if not filename.endswith(".sql"):
            continue

        object_name = os.path.basename(filename)
        object_name = os.path.splitext(object_name)[0].lower()

        filename = os.path.join(triggers_path, filename)
        with open(filename, 'r') as content_file:
            content = content_file.read()

        try:
            type_desc = find_object_type(content)
        except TypeError:
            print("Can't find type for {0}".format(object_name))
            raise

        result[object_name] = SqlObject(
            object_name=object_name,
            type_desc=type_desc,
            object_text=content)

    return result


def find_changed_objects(sql_objects, file_objects):
    result = []
    for key, val in file_objects.items():
        if key not in sql_objects:
            val.is_new = True
            result.append(val)
        else:
            if val.stripped_text != sql_objects[key].stripped_text:
                if val.type_desc != sql_objects[key].type_desc:
                    raise TypeError("{0}: {1} != {2}".format(
                        val.object_name, val.type_desc, sql_objects[key].type_desc))

                result.append(val)
    return result


def main(triggers_path, connection_string):
    # Read all triggers, procedures, etc...
    sql_objects = get_sql_objects(connection_string)
    file_objects = get_file_objects(triggers_path)

    # Iterate through all *.sql files, and find new ones.
    changed_objects = find_changed_objects(
        sql_objects=sql_objects,
        file_objects=file_objects)

    changed_objects.sort(key=lambda k: k.object_name)
    if not any(changed_objects):
        print("No changes found")
        return

    for co in changed_objects:
        if co.is_new:
            print("{0}new".format(co.object_name.ljust(80, '.')))
        else:
            print("{0}changed".format(co.object_name.ljust(80, '.')))

    # Create a result script.
    mega_update_script = []
    for co in changed_objects:
        mega_update_script.append(format_subquery(drop_if_exists_query(co), co.object_name))
        mega_update_script.append(format_subquery(co.object_text))
        mega_update_script.append(format_subquery(throw_if_not_exists_query(co)))

    mega_update_script = transaction_query_start + "\n".join(mega_update_script) + transaction_query_end

    # Run the result script.
    execute_sql_query(connection_string, mega_update_script)


class readable_dir(argparse.Action):
    def __call__(self,parser, namespace, values, option_string=None):
        prospective_dir=values
        if not os.path.isdir(prospective_dir):
            raise argparse.ArgumentTypeError("readable_dir:{0} is not a valid path".format(prospective_dir))
        if os.access(prospective_dir, os.R_OK):
            setattr(namespace,self.dest,prospective_dir)
        else:
            raise argparse.ArgumentTypeError("readable_dir:{0} is not a readable dir".format(prospective_dir))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--triggers_directory', action=readable_dir, default=TRIGGERS_PATH)
    parser.add_argument('-c', '--connection_string', default=CONNECTION_STRING)
    args = parser.parse_args()

    print("sql_triggers_update...")
    main(args.triggers_directory, args.connection_string)
    print("done")
