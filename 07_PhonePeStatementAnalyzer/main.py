import pypdf
import pandas as pd

all_text = pypdf.PdfReader("PhonePe_Statement_Apr2024_Oct2024.pdf")

# print(all_text.get_num_pages())
# c = 0
# for page in range(1,all_text.get_num_pages() -1):
#     text = all_text.pages[page].extract_text().split("\n")[4:][:-3]
#     for id, t in enumerate(text):
#         c += 1
#         print(id,t)

# print(c)

text = all_text.pages[10].extract_text().split("\n")[4:][:-3]
# text = all_text.pages[0].extract_text().split("\n")[6:][:-3]


def indexCreation(text):
    txn_list = []
    c=0
    for t in text:
        if c == 9:
            c = 0
        if c == 6 and len(t) > 20: 
            txn_list.append({c:t})
        else:
            c += 1
            txn_list.append({c:t})
    return txn_list

all_txn = []
for page in range(1,all_text.get_num_pages() -20):
    text = all_text.pages[page].extract_text().split("\n")[4:][:-3]
    all_txn.append(indexCreation(text))


all_txn_list = []
for i in all_txn:
    for j in i:
        all_txn_list.append(j)


print(pd.DataFrame(all_txn_list,))