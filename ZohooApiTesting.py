import cx_Oracle
import requests
import json
import time
import re

# ---------- Oracle configuration ----------
ORACLE_USER = "test"
ORACLE_PASS = "test"
ORACLE_DSN  = "localhost:1521/orcl"

# ---------- Zoho configuration ----------
account_owner = "alpha1.abdullah771"
form_branches = "Branches_Codes"
client_id="1000.4HCIPNZYPZK9NTGO3GMFVXWBE8E8VQ"
client_secret="6f0fcdc13edd87091ad54f57d41009042a4525f06d"
file =open("zoho_refresh_token.txt","r")
refresh_token=file.read().strip()
app_name = "carton"
form_name = "Items_Data"
response = requests.post("https://accounts.zoho.com/oauth/v2/token", params={
    "client_id":client_id,
    "client_secret": client_secret,
    "refresh_token": refresh_token,
    "grant_type": "refresh_token"
})
access_token = response.json()["access_token"]

# # ---------- Obtain Zoho access token ----------
# def get_access_token():
#     response = requests.post(
#         "https://accounts.zoho.com/oauth/v2/token",
#         params={
#             "client_id": client_id,
#             "client_secret": client_secret,
#             "refresh_token": refresh_token,
#             "grant_type": "refresh_token"
#         }
#     )

#     if response.status_code != 200:
#         raise Exception(f"Token request failed: {response.status_code} {response.text}")

#     data = response.json()
#     if "access_token" not in data:
#         raise Exception(f"OAuth Error: {data}")

#     access_token = data["access_token"]
#     print("Your Access Token Is: " + access_token)
#     return access_token

# ---------- Oracle connection ----------
def get_oracle_connection():
    return cx_Oracle.connect(ORACLE_USER, ORACLE_PASS, ORACLE_DSN, events=True)

# ---------- Alert callback ----------
def alert_callback(message):
    payload_str = message.payload
    print(f"\nAlert received: {payload_str}")

    rec_mode = re.search(r"<rec-mode>(.*?)</rec-mode>", payload_str).group(1)
    status   = re.search(r"<status>(.*?)</status>", payload_str).group(1)
    row_id   = int(re.search(r"<id>(.*?)</id>", payload_str).group(1))

    if message.name == "TEMP_SKM_INSERT":
        process_skm(row_id)
    elif message.name == "TEMP_GRB_INSERT":
        process_grb(row_id)

# ---------- Process a Temp_SKM row ----------
def process_skm(row_id):
    conn = get_oracle_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT SK1MCP, SK1MYR, SK1M1, SK1M2, SK1M3, SK1M9,
               SK1M11, SK1M12, SK1M13, SK1M14, SK1M16, SK1M17,
               SK1M18, SK1M19, SK1M20, SK1M21, SK1M22, SK1M24,
               SK1M29, SK1M31, SK1M32, SK1M33, SK1M34, SK1M36,
               SK1M37, SK1M39, SK1M40, SK1M41, SK1M261, PS33M2, PS33M4
        FROM Temp_SKM WHERE ID = :id
    """, id=row_id)
    row = cursor.fetchone()
    cursor.close()

    if not row:
        print(f"No row found with ID {row_id} in Temp_SKM")
        conn.close()
        return

    payload = {
        "data": {
            "Company"             : row[0],
            "Year"                : row[1],
            "Item_Code"           : row[2],
            "Item_Arabic_Name"    : row[3],
            "Item_English_Name"   : row[4],
            "SK1M9"               : row[5],
            "Posted_N_Year"       : row[6] == 'Y',
            "Imported_Item"       : row[7] == 'Y',
            "Expiry_Date"         : row[8] == 'Y',
            "Item_Tax"            : row[9],
            "Unit_1_Code"         : row[10],
            "Unit_2_Code"         : row[11],
            "Unit_3_Code"         : row[12],
            "Unit_4_Code"         : row[13],
            "Unit_1_Content"      : row[14],
            "Unit_2_Content"      : row[15],
            "Unit_3_Content"      : row[16],
            "Add_Code"            : row[17],
            "Parent_Item"         : row[18],
            "Salable_Item"        : row[19] == 'Y',
            "Purchasable_Item"    : row[20] == 'Y',
            "Productionable_Item" : row[21] == 'Y',
            "Report_Unit_Code"    : row[22],
            "Weight_Unit_Code"    : row[23],
            "Weight_of_Unit"      : row[24],
            "Sales_Unit_Code"     : row[25],
            "Batch_No"            : row[26] == 'Y',
            "Lot_No"              : row[27] == 'Y',
            "Eq_Code"             : row[28],
            "It_Filter_Type_Code" : row[29],
            "It_Filter_Code"      : row[30],
        }
    }

    status_code = post_to_zoho(account_owner, app_name, form_name, payload)

    new_status = 2 if status_code == 200 else 3
    cursor = conn.cursor()
    cursor.execute("UPDATE Temp_SKM SET status = :s WHERE ID = :id",
                   s=new_status, id=row_id)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Updated Temp_SKM ID {row_id} -> status {new_status}")

# ---------- Process a Temp_GRB row ----------
def process_grb(row_id):
    conn = get_oracle_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT GRBRCP, GRBRYR, BN, GRBR2, GRBR3
        FROM Temp_GRB WHERE ID = :id
    """, id=row_id)
    row = cursor.fetchone()
    cursor.close()

    if not row:
        print(f"No row found with ID {row_id} in Temp_GRB")
        conn.close()
        return

    payload = {
        "data": {
            "GRBRCP"               : row[0],
            "GRBRYR"               : row[1],
            "Branch_Code"          : row[2],
            "Branch_Arabic_Name"   : row[3],
            "Branch_English_Name"  : row[4],
        }
    }

    status_code = post_to_zoho(account_owner, app_name, form_branches, payload)

    new_status = 2 if status_code == 200 else 3
    cursor = conn.cursor()
    cursor.execute("UPDATE Temp_GRB SET status = :s WHERE ID = :id",
                   s=new_status, id=row_id)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Updated Temp_GRB ID {row_id} -> status {new_status}")

# ---------- Post to Zoho Creator (returns HTTP status code) ----------
def post_to_zoho(owner, app, form, payload):
    global access_token
    url = f"https://creatorapp.zoho.com/api/v2/{owner}/{app}/form/{form}"
    print("Calling URL:", url)
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json"
    }
    print(f"Posting to {form}: {json.dumps(payload, indent=2)}")
    json_string = json.dumps(payload, ensure_ascii=False).encode('utf-8')

    response = requests.post(url, headers=headers, data=json_string)

    if response.status_code == 201:
        print("Record Added Successfully! ✅")
        print(response.json())
    else:
        print("Something Went Wrong ❌")
        print("Status Code :", response.status_code)
        print("Error       :", response.json())
    return response.status_code

# ---------- Main listener ----------
def listen_alerts():
    conn = get_oracle_connection()
    cursor = conn.cursor()

    cursor.callproc("DBMS_ALERT.REGISTER", ("TEMP_SKM_INSERT",))
    cursor.callproc("DBMS_ALERT.REGISTER", ("TEMP_GRB_INSERT",))
    print("Waiting for Oracle alerts... (Ctrl+C to stop)")

    try:
        while True:
            name_var = cursor.var(cx_Oracle.STRING)
            msg_var  = cursor.var(cx_Oracle.STRING)
            status_var = cursor.var(cx_Oracle.NUMBER)

            cursor.callproc(
                "DBMS_ALERT.WAITANY",
                (name_var, msg_var, status_var, 5)
            )

            status = status_var.getvalue()

            if status == 0:
                alert_name = name_var.getvalue()
                payload    = msg_var.getvalue()

                print(f"\n📩 Alert received: {alert_name}")
                print(f"Payload: {payload}")

                class Message:
                    def __init__(self, name, payload):
                        self.name = name
                        self.payload = payload

                msg_obj = Message(alert_name, payload)

                try:
                    alert_callback(msg_obj)
                except Exception as e:
                    print("❌ Error in callback:", str(e))

            elif status == 1:
                pass  # timeout – normal
            else:
                print("⚠️ Unexpected status:", status)

    except KeyboardInterrupt:
        print("\nShutting down listener...")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    listen_alerts()