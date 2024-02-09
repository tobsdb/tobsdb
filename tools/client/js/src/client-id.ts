let id = 0;
export function GenClientId() {
  id++;
  return id;
}

export const ClientId = "__tdb_client_req_id__";
