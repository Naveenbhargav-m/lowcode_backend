
  function SetDataToGlobal() {
    let table = state["table"];
    let schema = state["schema"];
    let rows = state["rows"];



    let obj = {
        "schema": schema,
        "rows": rows,
        "table": table
      };
    return obj
}
SetDataToGlobal();


{
  "table": "users",
  "schema": "public",
  "rows": [
  {
    "id": "u1111",
    "email": "alice@example.com",
    "phone": "+1234567890",
    "password_hash": "hashed_password_1",
    "created_at": "2025-05-01T10:00:00Z",
    "last_login": "2025-05-10T12:00:00Z"
  },
  {
    "id": "u2222",
    "email": "bob@example.com",
    "phone": "+1987654321",
    "password_hash": "hashed_password_2",
    "created_at": "2025-05-02T11:00:00Z",
    "last_login": "2025-05-11T13:00:00Z"
  },
  {
    "id": "u3333",
    "email": "charlie@example.com",
    "phone": "+1122334455",
    "password_hash": "hashed_password_3",
    "created_at": "2025-05-03T09:30:00Z",
    "last_login": "2025-05-12T14:15:00Z"
  },
  {
    "id": "u4444",
    "email": "diana@example.com",
    "phone": "+1555666777",
    "password_hash": "hashed_password_4",
    "created_at": "2025-05-04T08:45:00Z",
    "last_login": "2025-05-13T10:20:00Z"
  },
  {
    "id": "u5555",
    "email": "evan@example.com",
    "phone": "+1888999000",
    "password_hash": "hashed_password_5",
    "created_at": "2025-05-05T14:30:00Z",
    "last_login": "2025-05-14T16:45:00Z"
  },
  {
    "id": "u6666",
    "email": "fiona@example.com",
    "phone": "+1777888999",
    "password_hash": "hashed_password_6",
    "created_at": "2025-05-06T11:15:00Z",
    "last_login": "2025-05-15T09:30:00Z"
  },
  {
    "id": "u7777",
    "email": "greg@example.com",
    "phone": "+1444555666",
    "password_hash": "hashed_password_7",
    "created_at": "2025-05-07T13:45:00Z",
    "last_login": "2025-05-14T18:20:00Z"
  },
  {
    "id": "u8888",
    "email": "hannah@example.com",
    "phone": "+1333222111",
    "password_hash": "hashed_password_8",
    "created_at": "2025-05-08T10:30:00Z",
    "last_login": "2025-05-15T11:45:00Z"
  },
  {
    "id": "u9999",
    "email": "ian@example.com",
    "phone": "+1222333444",
    "password_hash": "hashed_password_9",
    "created_at": "2025-05-09T15:15:00Z",
    "last_login": "2025-05-15T20:30:00Z"
  },
  {
    "id": "u1010",
    "email": "julia@example.com",
    "phone": "+1999888777",
    "password_hash": "hashed_password_10",
    "created_at": "2025-05-10T09:00:00Z",
    "last_login": "2025-05-16T08:15:00Z"
  }
]
}


let workflowconfig ={
  "name": "insert_rows",
  "start_block": "jsCode",
  "blocks": {
    "jsCode": {
    "name": "code_block",
    "handler": "code_block",
    "block_config": {
      "js_code": "function SetDataToGlobal() {\n    let table = state[\"table\"];\n    let schema = state[\"schema\"];\n    let rows = state[\"rows\"];\n\n    let obj = {\n        \"schema\": schema,\n        \"rows\": rows,\n        \"table\": table\n    };\n    return obj;\n}\nSetDataToGlobal(); "
    },
    "input_map": {
    },
    "next_block": ["insert_my_rows"]
    },
    "insert_my_rows": {
      "name": "insert_rows",
      "handler": "insert_rows",
      "input_map": {
        "table": `jsCode["table"]`,
        "schema": `jsCode["schema"]`,
        "rows": `jsCode["rows"]`
      },      
    },
    "end_block": {
    "name": "end_block",
    "handler": "noop"
    }
  },
  "end_block": "end_block"
  }