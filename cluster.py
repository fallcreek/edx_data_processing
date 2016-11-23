import pickle

data = pickle.load(open('Week7_7_cluster.pkl', 'rb'))

for key in data:
    if key != '9':
        continue
    print(key)
    print(data[key]["['R.1']"])
    # for val in data[key]:
    #     print(val)
    #     print(data[key][val])
