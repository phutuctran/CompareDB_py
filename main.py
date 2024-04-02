import pyodbc
import datetime
import uuid
import os

current_time = datetime.datetime.now()
formatted_time = current_time.strftime("%Y_%m_%d %H_%M_%S")
scriptDB_path = "./scriptDB.sql"
log = f"./Logs/{formatted_time}.txt"

def get_default_value(data_type):
    data_type_upper = data_type.upper()
    #print((data_type_upper))
    if data_type_upper in ['INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'FLOAT', 'REAL', 'DECIMAL', 'NUMERIC']:
        return 0
    elif data_type_upper in ['CHAR', 'VARCHAR', 'TEXT', 'NCHAR', 'NVARCHAR', 'NTEXT']:
        return "\'\'"
    elif data_type_upper in ['DATE', 'DATETIME', 'DATETIME2', 'SMALLDATETIME', 'TIME']:
        return "\'1900-01-01\'"
    elif data_type_upper == 'BIT':
        return False
    elif data_type_upper in ['BINARY', 'VARBINARY', 'IMAGE', 'UNIQUEIDENTIFIER']:
        return None
    elif data_type_upper == 'JSON':
        return "\'{}\'"
    else:
        return None

def get_precision_scale(conn, table, column):
    cursor = conn.cursor()
    query = f"SELECT NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' AND COLUMN_NAME = '{column}'"
    results = cursor.execute(query).fetchall()

    return [(result[0], result[1]) for result in results]
def get_db_name(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT DB_NAME() AS dbname")
    row = cursor.fetchone()
    return row.dbname

def get_all_tables(connection):
    cursor = connection.cursor()

    # Lấy tất cả các bảng trong cơ sở dữ liệu
    tables_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
    tables_result = cursor.execute(tables_query).fetchall()

    tables = {table[0] for table in tables_result}
    return tables

def get_table_columns(connection, table_name):
    cursor = connection.cursor()

    # Lấy thông tin cột của bảng
    columns_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    columns_result = cursor.execute(columns_query).fetchall()
    columns = {column[0] for column in columns_result}

    return columns

def get_table_constraints(connection, table_name):
    cursor = connection.cursor()

    # Truy vấn thông tin về các ràng buộc của bảng
    constraints_query = f"""
    SELECT KCU.CONSTRAINT_NAME, TC.CONSTRAINT_TYPE, KCU.COLUMN_NAME
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS as TC, INFORMATION_SCHEMA.KEY_COLUMN_USAGE as KCU
    WHERE KCU.TABLE_NAME = '{table_name}' and KCU.TABLE_NAME = TC.TABLE_NAME
    """
    constraints_result = cursor.execute(constraints_query).fetchall()

    # Trích xuất thông tin về ràng buộc từ kết quả truy vấn
    constraints = [(constraint[0], constraint[1], constraint[2]) for constraint in constraints_result]

    return constraints

def get_column_info(connection, table_name, column_name):
    cursor = connection.cursor()

    # Truy vấn thông tin về cột từ hệ thống thông tin cơ sở dữ liệu
    column_query = f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = '{column_name}'
    """
    cursor.execute(column_query)
    column_info_tmp = cursor.fetchone()

    #Truy vấn khóa chính
    primary_query = f"""
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
    AND TABLE_NAME = '{table_name}'
    AND COLUMN_NAME = '{column_name}'""";
    cursor.execute(primary_query)
    isPk = int(cursor.fetchone()[0]) > 0

    column_info = list([column_info_tmp[0], column_info_tmp[1], column_info_tmp[2], column_info_tmp[3], isPk])

    # Truy vấn thông tin về các ràng buộc khóa ngoại liên quan đến cột
    foreign_key_query = f"""
    SELECT
        fk.name AS constraint_name,
        tp.name AS referenced_table,
        cp.name AS referenced_column
    FROM 
        sys.foreign_keys fk
        INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.tables tp ON fkc.referenced_object_id = tp.object_id
        INNER JOIN sys.columns cp ON fkc.referenced_column_id = cp.column_id AND cp.object_id = tp.object_id
        INNER JOIN sys.columns cc ON fkc.parent_column_id = cc.column_id AND cc.object_id = tp.object_id
    WHERE
        cc.name = '{column_name}' AND tp.name = '{table_name}'
    """
    cursor.execute(foreign_key_query)
    foreign_key_info = cursor.fetchone()

    return column_info, foreign_key_info
def compare_database(conn1, conn2):
    # So sánh db2 với db1
    # Tìm sai, và tạo với các khác biệt của db1 với db2 nhưng ko xóa các dư thừa ở db2
    # Lấy tất cả các bảng trong cơ sở dữ liệu
    tables_result1 = get_all_tables(conn1)
    tables_result2 = get_all_tables(conn2)

    common_tables = tables_result1.intersection(tables_result2)
    for table in common_tables:
        write_file(log,"Table: " + table)

        #So sánh các columns trong table
        columns1 = get_table_columns(conn1, table)
        columns2 = get_table_columns(conn2, table)
        new_columns = columns2 - columns1
        if len(new_columns) == 0:
            write_file(log,"\tColumns no diff!")
            # So sánh các constranit trong table
            #constraints = compare_constraints(conn1, table, conn2, table)
            #print(constraints)
        else:
            for column in new_columns:
                write_file(log,f"\tNew column ({column})")
                write_file(scriptDB_path, create_column_script(conn2, table, get_column_info(conn2, table, column)[0]))
                #print(get_column_info(conn2, table, column))

    #print(common_tables)
    new_tables = tables_result2 - tables_result1
    for table in new_tables:
        write_file(log,"**New " + table)
        columns =get_table_columns(conn2, table)
        write_file(scriptDB_path,create_table_script(table))
        for column in columns:
            write_file(log,f"\tNew column ({column})")
            write_file(log, ' '.join(str(item) for item in get_column_info(conn2, table, column)))
            write_file(scriptDB_path,create_column_script(conn2, table, get_column_info(conn2, table, column)[0]))
        write_file(scriptDB_path,delete_column_script(table, "tmp"))


def compare_constraints(conn1, table_name1, conn2, table_name2):
    # Lấy danh sách các ràng buộc từ hai bảng
    constraints1 = get_table_constraints(conn1, table_name1)
    constraints2 = get_table_constraints(conn2, table_name2)

    # So sánh các ràng buộc
    common_constraints = []
    different_constraints1 = []

    for constraint2 in constraints2:
        found = False
        for constraint1 in constraints1:
            if constraint1[1:] == constraint2[1:]:
                common_constraints.append(constraint2)
                found = True
                break
        if not found:
            different_constraints1.append(constraint2)

    return common_constraints, different_constraints1

def create_column_script(conn, table_name, columnInfo):
    data_type = columnInfo[1].upper()
    script = f"ALTER TABLE [{table_name}] ADD {columnInfo[0]} {columnInfo[1]}"
    if columnInfo[2] != None:
        if columnInfo[2] == -1:
            script = script + "(MAX)"
        else:
            script = script + f"({columnInfo[2]})"
    elif data_type in ['DECIMAL', 'NUMERIC']:
        info = get_precision_scale(conn, table_name, columnInfo[0])
        script = script + f"{info[0]}"
    if columnInfo[3] == "NO":
        script = script + f" NOT NULL DEFAULT {get_default_value(columnInfo[1])}; "
    else:
        script = script + ";"

    if columnInfo[4] == True:
        script = script + f"\nALTER TABLE [{table_name}] ADD CONSTRAINT PK_{str(uuid.uuid4()).replace("-", "")} PRIMARY KEY ({columnInfo[0]});";

    return script

def create_table_script(table_name):
    script = f"CREATE TABLE [{table_name}] ( tmp int);"
    return script

def delete_column_script(table_name, column_name):
    script = f"ALTER TABLE [{table_name}] DROP COLUMN {column_name};"
    return script

def write_file(path, data):
    with open(path, 'a') as file:
        file.write(data + '\n')

def delete_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)

def main():
    # Kết nối đến cơ sở dữ liệu
    conn1 = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=.\\SQLEXPRESS;DATABASE=WatchStore')
    conn2 = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=.\\SQLEXPRESS;DATABASE=WatchStore2')
    delete_file(scriptDB_path)

    script = f"USE {get_db_name(conn1)}"
    write_file(scriptDB_path, script)

    compare_database(conn1, conn2)

    # Lặp qua từng bảng và lấy thông tin cột và ràng buộc của bảng đó
    """tables_result1 = get_all_tables(conn1)
    for table in tables_result1:
        print("Table:", table)

        columns= get_table_columns(conn1, table)

        print("Columns:")
        for column in columns:
            print("  " + column)

        print("\n")"""

    # Đóng kết nối
    conn1.close()
    conn2.close()


if __name__ == "__main__":
    main()