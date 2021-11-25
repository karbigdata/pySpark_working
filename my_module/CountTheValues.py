
def countFreq( arr, n):
    mp = dict()

    for i in range(n):
        if arr[i] in mp.keys():
            mp[arr[i]] += 1
        else:
            mp[arr[i]] = 1

    for x in mp:
        print(x, " ", mp[x])




data = [1,1,2,3,3, 10,10,2]
n = len(data)
countFreq(data, n)
