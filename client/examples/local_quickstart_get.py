import featureform as ff

ff.register_user("featureformer").make_default_owner()

local = ff.register_local()

local_provider_get = ff.get_local_provider()

transactions = local_provider_get.register_file(
    name="transactions",
    variant="quickstart",
    description="A dataset of fraudulent transactions",
    path="transactions.csv",
)


@local_provider_get.df_transformation(
    variant="quickstart", inputs=[("transactions", "quickstart")]
)
def average_user_transaction(transactions):
    """the average transaction amount for a user"""
    return transactions.groupby("CustomerID")["TransactionAmount"].mean()


average_user_transaction_get = ff.get_source(
    "average_user_transaction", "quickstart", True
)

user = ff.register_entity("user")

user_get = ff.get_entity("user")
# Register a column from our transformation as a feature

average_user_transaction_get.register_resources(
    entity=user_get,
    entity_column="CustomerID",
    inference_store=local_provider_get,
    features=[
        {
            "name": "avg_transactions",
            "variant": "quickstart",
            "column": "TransactionAmount",
            "type": "float32",
        },
        {
            "name": "avg_transactions_2",
            "variant": "quickstart",
            "column": "TransactionAmount",
            "type": "float32",
        },
    ],
)

transactions_get = ff.get_source("transactions", "quickstart", True)

# Register label from our base Transactions table
transactions_get.register_resources(
    entity=user_get,
    entity_column="CustomerID",
    labels=[
        {
            "name": "fraudulent",
            "variant": "quickstart",
            "column": "IsFraud",
            "type": "bool",
        },
    ],
)

ff.register_training_set(
    "fraud_training",
    "quickstart",
    label=("fraudulent", "quickstart"),
    features=[("avg_transactions", "quickstart")],
)
