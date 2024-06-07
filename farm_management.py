import argparse
import mysql.connector
from mysql.connector import Error


class FarmManagementDB:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                auth_plugin='mysql_native_password'
            )
            if self.connection.is_connected():
                print("Successfully connected to the database")
        except Error as e:
            print(f"Error: {e}")

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Connection closed")

    def add_farmer(self, name, address):
        if not self.connection or not self.connection.is_connected():
            print("Not connected to the database")
            return
        try:
            cursor = self.connection.cursor()
            query = "INSERT INTO farmers (name, address) VALUES (%s, %s)"
            cursor.execute(query, (name, address))
            self.connection.commit()
            print("Farmer added successfully")
        except Error as e:
            print(f"Error: {e}")

    def update_farmer(self, farmer_id, name=None, address=None):
        if not self.connection or not self.connection.is_connected():
            print("Not connected to the database")
            return
        try:
            cursor = self.connection.cursor()
            query = "UPDATE farmers SET "
            updates = []
            params = []
            if name:
                updates.append("name = %s")
                params.append(name)
            if address:
                updates.append("address = %s")
                params.append(address)
            query += ", ".join(updates)
            query += " WHERE id = %s"
            params.append(farmer_id)
            cursor.execute(query, params)
            self.connection.commit()
            print("Farmer updated successfully")
        except Error as e:
            print(f"Error: {e}")

    def delete_farmer(self, farmer_id):
        if not self.connection or not self.connection.is_connected():
            print("Not connected to the database")
            return
        try:
            cursor = self.connection.cursor()
            # Удаление связанных записей из таблицы needs
            query = "DELETE FROM needs WHERE farmer_id = %s"
            cursor.execute(query, (farmer_id,))
            # Удаление связанных записей из таблицы products
            query = "DELETE FROM products WHERE farmer_id = %s"
            cursor.execute(query, (farmer_id,))
            # Удаление фермера
            query = "DELETE FROM farmers WHERE id = %s"
            cursor.execute(query, (farmer_id,))
            self.connection.commit()
            print("Farmer deleted successfully")
        except Error as e:
            print(f"Error: {e}")

    def add_product(self, farmer_id, name, quantity, quality, price):
        if not self.connection or not self.connection.is_connected():
            print("Not connected to the database")
            return
        try:
            cursor = self.connection.cursor()
            # Проверка существования фермера
            query = "SELECT id FROM farmers WHERE id = %s"
            cursor.execute(query, (farmer_id,))
            result = cursor.fetchone()
            if result is None:
                print(f"Farmer with ID {farmer_id} does not exist")
                return
            # Добавление продукта
            query = "INSERT INTO products (farmer_id, name, quantity, quality, price) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(query, (farmer_id, name, quantity, quality, price))
            self.connection.commit()
            print("Product added successfully")
        except Error as e:
            print(f"Error: {e}")

    def update_product(self, product_id, name=None, quantity=None, quality=None, price=None):
        if not self.connection or not self.connection.is_connected():
            print("Not connected to the database")
            return
        try:
            cursor = self.connection.cursor()
            query = "UPDATE products SET "
            updates = []
            params = []
            if name:
                updates.append("name = %s")
                params.append(name)
            if quantity is not None:
                updates.append("quantity = %s")
                params.append(quantity)
            if quality:
                updates.append("quality = %s")
                params.append(quality)
            if price is not None:
                updates.append("price = %s")
                params.append(price)
            query += ", ".join(updates)
            query += " WHERE id = %s"
            params.append(product_id)
            cursor.execute(query, params)
            self.connection.commit()
            print("Product updated successfully")
        except Error as e:
            print(f"Error: {e}")

    def delete_product(self, product_id):
        if not self.connection or not self.connection.is_connected():
            print("Not connected to the database")
            return
        try:
            cursor = self.connection.cursor()
            query = "DELETE FROM products WHERE id = %s"
            cursor.execute(query, (product_id,))
            self.connection.commit()
            print("Product deleted successfully")
        except Error as e:
            print(f"Error: {e}")

    def add_need(self, farmer_id, name, type_, price):
        if not self.connection or not self.connection.is_connected():
            print("Not connected to the database")
            return
        try:
            cursor = self.connection.cursor()
            query = "INSERT INTO needs (farmer_id, name, type, price) VALUES (%s, %s, %s, %s)"
            cursor.execute(query, (farmer_id, name, type_, price))
            self.connection.commit()
            print("Need added successfully")
        except Error as e:
            print(f"Error: {e}")

    def update_need(self, need_id, name=None, type_=None, price=None):
        if not self.connection or not self.connection.is_connected():
            print("Not connected to the database")
            return
        try:
            cursor = self.connection.cursor()
            query = "UPDATE needs SET "
            updates = []
            params = []
            if name:
                updates.append("name = %s")
                params.append(name)
            if type_:
                updates.append("type = %s")
                params.append(type_)
            if price is not None:
                updates.append("price = %s")
                params.append(price)
            query += ", ".join(updates)
            query += " WHERE id = %s"
            params.append(need_id)
            cursor.execute(query, params)
            self.connection.commit()
            print("Need updated successfully")
        except Error as e:
            print(f"Error: {e}")

    def delete_need(self, need_id):
        if not self.connection or not self.connection.is_connected():
            print("Not connected to the database")
            return
        try:
            cursor = self.connection.cursor()
            query = "DELETE FROM needs WHERE id = %s"
            cursor.execute(query, (need_id,))
            self.connection.commit()
            print("Need deleted successfully")
        except Error as e:
            print(f"Error: {e}")

    def get_products_by_farmer(self):
        if not self.connection or not self.connection.is_connected():
            print("Not connected to the database")
            return
        try:
            cursor = self.connection.cursor(dictionary=True)
            query = """
            SELECT f.name AS farmer_name, p.name AS product_name, p.quantity, p.quality, p.price
            FROM farmers f
            JOIN products p ON f.id = p.farmer_id;
            """
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Error as e:
            print(f"Error: {e}")

    def get_needs_by_farmer(self):
        if not self.connection or not self.connection.is_connected():
            print("Not connected to the database")
            return
        try:
            cursor = self.connection.cursor(dictionary=True)
            query = """
            SELECT f.name AS farmer_name, n.name AS need_name, n.type, n.price
            FROM farmers f
            JOIN needs n ON f.id = n.farmer_id;
            """
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Error as e:
            print(f"Error: {e}")

    def get_total_product_quantity(self, product_name):
        if not self.connection or not self.connection.is_connected():
            print("Not connected to the database")
            return
        try:
            cursor = self.connection.cursor(dictionary=True)
            query = """
            SELECT p.name AS product_name, SUM(p.quantity) AS total_quantity
            FROM products p
            WHERE p.name = %s
            GROUP BY p.name;
            """
            cursor.execute(query, (product_name,))
            results = cursor.fetchall()
            return results
        except Error as e:
            print(f"Error: {e}")

    def get_farmer_profit(self):
        if not self.connection or not self.connection.is_connected():
            print("Not connected to the database")
            return
        try:
            cursor = self.connection.cursor(dictionary=True)
            query = """
            SELECT f.name AS farmer_name, p.name AS product_name, SUM(p.quantity * p.price) AS total_profit
            FROM farmers f
            JOIN products p ON f.id = p.farmer_id
            GROUP BY f.name, p.name;
            """
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Error as e:
            print(f"Error: {e}")

    def get_farmer_credit(self):
        if not self.connection or not self.connection.is_connected():
            print("Not connected to the database")
            return
        try:
            cursor = self.connection.cursor(dictionary=True)
            query = """
            SELECT f.name AS farmer_name, SUM(n.price) AS total_credit
            FROM farmers f
            JOIN needs n ON f.id = n.farmer_id
            GROUP BY f.name;
            """
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Error as e:
            print(f"Error: {e}")

    def get_profit_vs_credit(self):
        if not self.connection or not self.connection.is_connected():
            print("Not connected to the database")
            return
        try:
            cursor = self.connection.cursor(dictionary=True)
            query = """
            SELECT 
                f.name AS farmer_name,
                (SELECT SUM(p.quantity * p.price) FROM products p WHERE p.farmer_id = f.id) AS total_profit,
                (SELECT SUM(n.price) FROM needs n WHERE n.farmer_id = f.id) AS total_credit,
                ((SELECT SUM(p.quantity * p.price) FROM products p WHERE p.farmer_id = f.id) - 
                 (SELECT SUM(n.price) FROM needs n WHERE n.farmer_id = f.id)) AS difference
            FROM farmers f;
            """
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Error as e:
            print(f"Error: {e}")


def main():
    parser = argparse.ArgumentParser(description="Farm Management System")
    parser.add_argument("action", choices=["add_farmer", "update_farmer", "delete_farmer",
                                           "add_product", "update_product", "delete_product",
                                           "add_need", "update_need", "delete_need",
                                           "get_products", "get_needs",
                                           "get_total_product_quantity", "get_farmer_profit",
                                           "get_farmer_credit", "get_profit_vs_credit"],
                        help="Action to perform")
    parser.add_argument("--host", default="localhost", help="Database host")
    parser.add_argument("--user", default="root", help="Database user")
    parser.add_argument("--password", default="root", help="Database password")
    parser.add_argument("--database", default="farm_management", help="Database name")

    parser.add_argument("--farmer_id", type=int, help="Farmer ID")
    parser.add_argument("--farmer_name", help="Farmer Name")
    parser.add_argument("--address", help="Farmer Address")

    parser.add_argument("--product_id", type=int, help="Product ID")
    parser.add_argument("--product_farmer_id", type=int, help="Farmer ID for Product")
    parser.add_argument("--product_name", help="Product Name")
    parser.add_argument("--quantity", type=int, help="Quantity")
    parser.add_argument("--quality", help="Quality")
    parser.add_argument("--price", type=float, help="Price")

    # Need related arguments
    parser.add_argument("--need_id", type=int, help="Need ID")
    parser.add_argument("--need_farmer_id", type=int, help="Farmer ID for Need")
    parser.add_argument("--need_name", help="Need Name")
    parser.add_argument("--type", help="Need Type")
    parser.add_argument("--need_price", type=float, help="Need Price")

    args = parser.parse_args()

    db = FarmManagementDB(host=args.host, user=args.user, password=args.password, database=args.database)
    db.connect()

    if args.action == "add_farmer":
        db.add_farmer(name=args.farmer_name, address=args.address)
    elif args.action == "update_farmer":
        db.update_farmer(farmer_id=args.farmer_id, name=args.farmer_name, address=args.address)
    elif args.action == "delete_farmer":
        db.delete_farmer(farmer_id=args.farmer_id)
    elif args.action == "add_product":
        db.add_product(farmer_id=args.product_farmer_id, name=args.product_name, quantity=args.quantity,
                       quality=args.quality, price=args.price)
    elif args.action == "update_product":
        db.update_product(product_id=args.product_id, name=args.product_name, quantity=args.quantity,
                          quality=args.quality, price=args.price)
    elif args.action == "delete_product":
        db.delete_product(product_id=args.product_id)
    elif args.action == "add_need":
        db.add_need(farmer_id=args.need_farmer_id, name=args.need_name, type_=args.type, price=args.need_price)
    elif args.action == "update_need":
        db.update_need(need_id=args.need_id, name=args.need_name, type_=args.type, price=args.need_price)
    elif args.action == "delete_need":
        db.delete_need(need_id=args.need_id)
    elif args.action == "get_products":
        products = db.get_products_by_farmer()
        for product in products:
            print(product)
    elif args.action == "get_needs":
        needs = db.get_needs_by_farmer()
        for need in needs:
            print(need)
    elif args.action == "get_total_product_quantity":
        total_quantity = db.get_total_product_quantity(product_name=args.product_name)
        for tq in total_quantity:
            print(tq)
    elif args.action == "get_farmer_profit":
        profit = db.get_farmer_profit()
        for p in profit:
            print(p)
    elif args.action == "get_farmer_credit":
        credit = db.get_farmer_credit()
        for c in credit:
            print(c)
    elif args.action == "get_profit_vs_credit":
        profit_vs_credit = db.get_profit_vs_credit()
        for pvc in profit_vs_credit:
            print(pvc)

    db.disconnect()


if __name__ == "__main__":
    main()
