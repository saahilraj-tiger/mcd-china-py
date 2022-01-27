import yaml
import os

print(os.getcwd())

with open('master.yml', 'r') as file:
    prime_service = yaml.safe_load_all(file)

    for i in prime_service:
        print(i)
