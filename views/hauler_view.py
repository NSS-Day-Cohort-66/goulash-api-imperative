import sqlite3
import json


def update_hauler(id, hauler_data):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
            UPDATE Hauler
                SET
                    name = ?,
                    dock_id = ?
            WHERE id = ?
            """,
            (hauler_data["name"], hauler_data["dock_id"], id),
        )

        rows_affected = db_cursor.rowcount

    return True if rows_affected > 0 else False


def delete_hauler(pk):
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        db_cursor.execute(
            """
        DELETE FROM Hauler WHERE id = ?
        """,
            (pk,),
        )
        number_of_rows_deleted = db_cursor.rowcount

    return True if number_of_rows_deleted > 0 else False


def list_haulers(query_params):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        if "_expand" in query_params:
            db_cursor.execute(
                """
            SELECT
                h.id,
                h.name,
                h.dock_id,
                d.id dockId,
                d.location,
                d.capacity
            FROM Hauler h
            JOIN Dock d
                ON d.id = h.dock_id
            """
            )
        else:
            db_cursor.execute(
                """
            SELECT
                h.id,
                h.name,
                h.dock_id
            FROM Hauler h
            """
            )
        query_results = db_cursor.fetchall()

        # Initialize an empty list and then add each dictionary to it
        haulers = []
        for row in query_results:
            if "_expand" in query_params:
                dock = {
                    "id": row["dockId"],
                    "location": row["location"],
                    "capacity": row["capacity"],
                }
                hauler = {
                    "id": row["id"],
                    "name": row["name"],
                    "dock_id": row["dock_id"],
                    "dock": dock,
                }
                haulers.append(hauler)
            else:
                haulers.append(dict(row))

        # Serialize Python list to JSON encoded string
        serialized_haulers = json.dumps(haulers)

    return serialized_haulers


def retrieve_hauler(query_params, pk):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        if "_expand" in query_params:
            db_cursor.execute(
                """
            SELECT
                h.id,
                h.name,
                h.dock_id,
                d.id dockId,
                d.location,
                d.capacity
            FROM Hauler h
            JOIN Dock d
                ON d.id = h.dock_id
            WHERE h.id = ?
            """,
                (pk,),
            )
        else:
            db_cursor.execute(
                """
            SELECT
                h.id,
                h.name,
                h.dock_id
            FROM Hauler h
            WHERE h.id = ?
            """,
                (pk,),
            )
        query_results = db_cursor.fetchone()

        if "_expand" in query_params:
            dock = {
                "id": query_results["dockId"],
                "location": query_results["location"],
                "capacity": query_results["capacity"],
            }
            hauler = {
                "id": query_results["id"],
                "name": query_results["name"],
                "dock_id": query_results["dock_id"],
                "dock": dock,
            }
            # Serialize Python list to JSON encoded string
            serialized_hauler = json.dumps(hauler)
        else:
            # Serialize Python list to JSON encoded string
            serialized_hauler = json.dumps(dict(query_results))

    return serialized_hauler
