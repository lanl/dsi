import pandas as pd
from sklearn.model_selection import train_test_split

file_path = '/Users/abarib/Desktop/Internship_Docs_and_Data/Code_and_Data/Python_code/Text_Vis/NVBench_updated.csv'
df = pd.read_csv(file_path)

#unique categories
categories = df['db_id'].unique()
chart_types = df['chart'].unique()

#Split categories into seen and unseen
seen_categories, unseen_categories = train_test_split(categories, test_size=0.2, random_state=42)

# Split
train_df = df[df['db_id'].isin(seen_categories)]
test_seen_df = train_df.sample(frac=0.2, random_state=42)
train_df = train_df.drop(test_seen_df.index)

# testing set with unseen categories
test_unseen_df = df[df['db_id'].isin(unseen_categories)]

#stratifies split
def even_distribution(df, column, target_ratio=0.2):
    train_dfs = []
    test_dfs = []
    for value in df[column].unique():
        value_df = df[df[column] == value]
        train, test = train_test_split(value_df, test_size=target_ratio, random_state=42)
        train_dfs.append(train)
        test_dfs.append(test)
    return pd.concat(train_dfs), pd.concat(test_dfs)

train_df, test_seen_df = even_distribution(train_df, 'chart')

#  counts for seen categories
seen_category_counts_train = train_df['db_id'].value_counts()
seen_category_counts_test = test_seen_df['db_id'].value_counts()

# counts for unseen categories
unseen_category_counts = test_unseen_df['db_id'].value_counts()

#categories
print("Seen Categories and Counts in Training Set:")
print(seen_category_counts_train)
print("\nSeen Categories and Counts in Test Set:")
print(seen_category_counts_test)
print("\nUnseen Categories and Counts in Test Set:")
print(unseen_category_counts)

#chart types for training, seen test, and unseen test sets
print("\nChart Types in Training Set:")
print(train_df['chart'].value_counts())
print("\nChart Types in Seen Test Set:")
print(test_seen_df['chart'].value_counts())
print("\nChart Types in Unseen Test Set:")
print(test_unseen_df['chart'].value_counts())
#hardness
print("\nHardness of Data Points in Training Set:")
print(train_df['hardness'].value_counts())
print("\nHardness of Data Points in Seen Test Set:")
print(test_seen_df['hardness'].value_counts())
print("\nHardness of Data Points in Unseen Test Set:")
print(test_unseen_df['hardness'].value_counts())

train_df.to_csv('train_set.csv', index=False)
test_seen_df.to_csv('test_seen_set.csv', index=False)
test_unseen_df.to_csv('test_unseen_set.csv', index=False)
