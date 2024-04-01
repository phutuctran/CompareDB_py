import pyodbc

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

def compare_database(conn1, conn2):
    # So sánh db2 với db1
    # Tìm sai, và tạo với các khác biệt của db1 với db2 nhưng ko xóa các dư thừa ở db2
    # Lấy tất cả các bảng trong cơ sở dữ liệu
    tables_result1 = get_all_tables(conn1)
    tables_result2 = get_all_tables(conn2)

    common_tables = tables_result1.intersection(tables_result2)
    for table in common_tables:
        print("Table: " + table)

        #So sánh các columns trong table
        columns1 = get_table_columns(conn1, table)
        columns2 = get_table_columns(conn2, table)
        #print(columns1)
        #print(columns2)
        new_columns = columns2 - columns1
        if len(new_columns) == 0:
            print("\tColumns no diff!")
            # So sánh các constranit trong table
            constraints = compare_constraints(conn1, table, conn2, table)
            print(constraints)
        else:
            for column in new_columns:
                print(f"\tNew column ({column})")




    #print(common_tables)

    new_tables = tables_result2 - tables_result1
    return new_tables


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

def main():
    # Kết nối đến cơ sở dữ liệu
    conn1 = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=.\\SQLEXPRESS;DATABASE=WatchStore')
    conn2 = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=.\\SQLEXPRESS;DATABASE=WatchStore2')

    tables = compare_database(conn1, conn2)
    print(tables)



    # Lặp qua từng bảng và lấy thông tin cột và ràng buộc của bảng đó
    """for table in tables_result1:
        print("Table:", table)

        columns, constraints = get_table_info(conn1, table)

        print("Columns:")
        for column in columns:
            print("  " + column)

        print("Constraints:")
        for constraint in constraints:
            print(constraint[0], constraint[1])

        print("\n")"""

    # Đóng kết nối
    conn1.close()
    conn2.close()


if __name__ == "__main__":
    main()