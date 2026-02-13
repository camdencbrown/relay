import requests, json

r = requests.post('http://localhost:8001/api/v1/pipeline/create', json={
    'name': 'Test Pipeline',
    'source': {'type': 'csv_url', 'url': 'http://localhost:8002/customers.csv'},
    'destination': {'type': 's3', 'bucket': 'airbyte-poc-bucket-cb', 'path': 'relay/test'}
})

print("Status:", r.status_code)
print("Response:")
print(json.dumps(r.json(), indent=2))
