from db_cleanup import DBManager

tables_ = [
    {
        'name': 'cart_product_addon',
        'temp': 'tmp_cart_product_addon_cleanup'
    },
    {
        'name': 'cart_product',
        'temp': 'tmp_cart_product_cleanup_1'
    },
    {
        'name': 'cart_product',
        'temp': 'tmp_cart_product_cleanup_2'
    },
    {
        'name': 'carts',
        'temp': 'tmp_carts_cleanup'
    },
    {
        'name': 'delivery_information',
        'temp': 'tmp_delivery_information_cleanup'
    },
    {
        'name': 'registration_information',
        'temp': 'tmp_registeration_information_cleanup'
    },
    {
        'name': 'transactions',
        'temp': 'tmp_transaction_cleanup'
    },
    {
        'name': 'users',
        'temp': 'tmp_users_cleanup'
    }
]

config = {
    'queries': tables_,
    'db_url': 'postgresql://user:password@ip/db',
    'deletion_limit': 2000,
    'echo': False,
}

db_manager = DBManager(tables=config['queries'], db_url=config['db_url'])
db_manager.run_cleanup()
