# =============================================
# Script de Sincronização entre dois bancos PostgreSQL
# Empresa: IARA
# Descrição: Copia e atualiza dados de várias tabelas do banco de origem (1º ano)
#            para o banco de destino (2º ano), mantendo consistência via UPSERT.
# =============================================

import psycopg2                    # Biblioteca para conectar e executar queries no PostgreSQL
from dotenv import load_dotenv     # Carrega variáveis de ambiente do arquivo .env
from os import getenv               # Permite acessar variáveis do sistema
from datetime import datetime, timezone  # Manipula datas e fusos horários

# Carrega as variáveis de ambiente (.env)
load_dotenv()

# =====================================================
# Conexão com o banco de ORIGEM (1º ano)
# =====================================================
conn1 = psycopg2.connect(
    dbname=getenv("DB_NAME_FIRST"),
    user=getenv("DB_USER"),
    password=getenv("DB_PASSWORD"),
    host=getenv("DB_HOST"),
    port=getenv("DB_PORT")
)
cur1 = conn1.cursor()  # Cria o cursor para executar comandos SQL no banco de origem

# =====================================================
# Conexão com o banco de DESTINO (2º ano)
# =====================================================
conn2 = psycopg2.connect(
    dbname=getenv("DB_NAME_SECOND"),
    user=getenv("DB_USER"),
    password=getenv("DB_PASSWORD"),
    host=getenv("DB_HOST"),
    port=getenv("DB_PORT")
)
cur2 = conn2.cursor()  # Cria o cursor para o banco de destino

# =====================================================
# Sincronização da tabela "Fábrica"
# =====================================================
cur1.execute("SELECT id, nome_unidade, cnpj_unidade, email_corporativo, status, ramo FROM fabrica;")
factory_records = cur1.fetchall()  # Busca todos os registros da tabela de origem

for factory_id, name, cnpj, email, status, description in factory_records:
    # Extrai o domínio do e-mail corporativo (após o @)
    domain = email.split('@')[1] if email and '@' in email else ''
    # Define a data de desativação caso o status seja falso (fábrica inativa)
    deactivated_at = datetime.now() if not status else None

    # Insere ou atualiza os dados no banco de destino
    cur2.execute("""
        INSERT INTO factory (pk_id, cnpj, name, domain, deactivated_at, description)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (pk_id)
        DO UPDATE SET cnpj = EXCLUDED.cnpj,
                      name = EXCLUDED.name,
                      domain = EXCLUDED.domain,
                      deactivated_at = EXCLUDED.deactivated_at,
                      description = EXCLUDED.description;
    """, (factory_id, cnpj, name, domain, deactivated_at, description))

print(f"Factory: {len(factory_records)} synchronized records.")

# =====================================================
# Sincronização da tabela "Endereço"
# =====================================================
cur1.execute("SELECT id, cep, estado, cidade, bairro, rua, numero, complemento, fk_fabrica FROM endereco;")
address_records = cur1.fetchall()

for r in address_records:
    # Insere ou atualiza o endereço vinculado à fábrica
    cur2.execute("""
        INSERT INTO address (pk_id, cep, state, city, neighborhood, street, building_number, complement, factory_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (pk_id)
        DO UPDATE SET cep = EXCLUDED.cep,
                      state = EXCLUDED.state,
                      city = EXCLUDED.city,
                      neighborhood = EXCLUDED.neighborhood,
                      street = EXCLUDED.street,
                      building_number = EXCLUDED.building_number,
                      complement = EXCLUDED.complement,
                      factory_id = EXCLUDED.factory_id;
    """, r)

print(f"Address: {len(address_records)} synchronized records.")

# =====================================================
# Sincronização da tabela "Gênero"
# =====================================================
cur1.execute("SELECT DISTINCT genero FROM usuario WHERE genero IS NOT NULL;")
gender_records = cur1.fetchall()

gender_map = {}

for i, (gender_name,) in enumerate(gender_records, start=1):
    gender_map[gender_name] = i
    if gender_name == 'masc':
        gender_name = 'Masculino'
    else:
        gender_name = 'Feminino'

    cur2.execute("""
        INSERT INTO gender (pk_id, name)
        VALUES (%s, %s)
        ON CONFLICT (pk_id)
        DO UPDATE SET name = EXCLUDED.name;
    """, (i, gender_name))

print(f"Gender: {len(gender_records)} synchronized records.")

# =====================================================
# Sincronização da tabela "Usuário"
# =====================================================
cur1.execute("""
    SELECT id, nome, email, senha, data_criacao, data_nascimento, status, genero, id_gerente, fk_fabrica, tipo_acesso, desc_tipoacesso
    FROM usuario;
""")
user_account_records = cur1.fetchall()

for r in user_account_records:
    # Define a data de desativação se o status não for 'Ativo'
    deactivated_at = datetime.now() if r[6] != 'Ativo' else None
    # Ajusta o campo data_criacao para UTC
    created_at = datetime.combine(r[4], datetime.min.time(), tzinfo=timezone.utc)
    # Pega o ID da tabela de gênero com base na coluna 'gênero' no banco de origem
    manager_uuid = r[8] if r[8] else None

    gender_id = gender_map.get(r[7])

    # Insere ou atualiza o usuário no destino
    cur2.execute("""
        INSERT INTO user_account (pk_uuid, name, email, password, created_at, date_of_birth,
                                  deactivated_at, gender_id, user_manager_uuid, factory_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (pk_uuid)
        DO UPDATE SET name = EXCLUDED.name,
                      email = EXCLUDED.email,
                      password = EXCLUDED.password,
                      created_at = EXCLUDED.created_at,
                      date_of_birth = EXCLUDED.date_of_birth,
                      deactivated_at = EXCLUDED.deactivated_at,
                      gender_id = EXCLUDED.gender_id,
                      user_manager_uuid = EXCLUDED.user_manager_uuid,
                      factory_id = EXCLUDED.factory_id;
    """, (r[0], r[1], r[2], r[3], created_at, r[5], deactivated_at, gender_id, manager_uuid, r[9]))

print(f"User Account: {len(user_account_records)} synchronized records.")

# =====================================================
# Sincronização da tabela "Tipo de Acesso"
# =====================================================

# Cria um conjunto com combinações únicas de tipo e descrição
access_type_records = {(u[10], u[11]) for u in user_account_records if u[10] and u[11]}

for type, description in access_type_records:
    # Insere ou atualiza os tipos de acesso
    cur2.execute("""
        INSERT INTO access_type (name, description)
        VALUES (%s, %s)
        ON CONFLICT (name)
            DO UPDATE SET description = EXCLUDED.description;
    """, (type, description))

print(f"Access Type: {len(access_type_records)} synchronized records.")

# Mapeia tipos de acesso para seus IDs no destino
cur2.execute("SELECT pk_id, name FROM access_type;")
map_types = {name: pk_id for pk_id, name in cur2.fetchall()}

# Relaciona usuários aos seus tipos de acesso
for u in user_account_records:
    user_uuid = u[0]
    access_type = u[10]

    if access_type in map_types:
        access_type_id = map_types[access_type]
        cur2.execute("""
            INSERT INTO user_account_access_type (user_account_uuid, access_type_id)
            VALUES (%s, %s)
            ON CONFLICT (user_account_uuid, access_type_id)
            DO UPDATE SET created_at = EXCLUDED.created_at,
                          deactivated_at = EXCLUDED.deactivated_at;
        """, (user_uuid, access_type_id))

print("User Account -> Access Type synchronized records.")

# =====================================================
# Sincronização da tabela "Plano"
# =====================================================
cur1.execute("SELECT id, nome, valor, descricao, duracao FROM plano;")
registros_planos = cur1.fetchall()

for r in registros_planos:
    delta = r[4]
    total_days = delta.days

    r = list(r)  # tuplas são imutáveis
    r[4] = total_days // 30 

    # Insere ou atualiza os planos de assinatura
    cur2.execute("""
        INSERT INTO subscription (pk_id, name, price, description, monthly_duration)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (pk_id)
        DO UPDATE SET name = EXCLUDED.name,
                      price = EXCLUDED.price,
                      description = EXCLUDED.description,
                      monthly_duration = EXCLUDED.monthly_duration;
    """, r)

print(f"Subscription: {len(registros_planos)} registros sincronizados.")

# =====================================================
# Sincronização da tabela "Método de Pagamento"
# =====================================================
cur1.execute("SELECT id, tipo_pagamento FROM metodo_pagamento;")
payment_method_records = cur1.fetchall()

for r in payment_method_records:
    # Insere ou atualiza o método de pagamento
    cur2.execute("""
        INSERT INTO payment_method (pk_id, name)
        VALUES (%s, %s)
        ON CONFLICT (pk_id)
        DO UPDATE SET name = EXCLUDED.name;
    """, r)

print(f"Payment Method: {len(payment_method_records)} synchronized records.")

# =====================================================
# Sincronização da tabela "Pagamento"
# =====================================================
cur1.execute("""
    SELECT id, valor, data_pagamento, data_inicio, data_vencimento, status,
           fk_plano, fk_usuario, fk_metodo_pagamento
    FROM pagamento;
""")
payment_records = cur1.fetchall()

for id, total, paid_at, starts_at, expires_on, status, subscription_id, user_uuid, payment_method_id in payment_records:
    # Define flags de ativo/expirado baseadas no status
    is_expired = not status
    is_active = status

    # Insere ou atualiza os pagamentos no destino
    cur2.execute("""
        INSERT INTO payment (pk_id, total, paid_at, starts_at, expires_on,
                             is_active, is_expired, subscription_id,
                             user_account_uuid, payment_method_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (pk_id)
        DO UPDATE SET total = EXCLUDED.total,
                      paid_at = EXCLUDED.paid_at,
                      starts_at = EXCLUDED.starts_at,
                      expires_on = EXCLUDED.expires_on,
                      is_active = EXCLUDED.is_active,
                      is_expired = EXCLUDED.is_expired,
                      subscription_id = EXCLUDED.subscription_id,
                      user_account_uuid = EXCLUDED.user_account_uuid,
                      payment_method_id = EXCLUDED.payment_method_id;
    """, (id, total, paid_at, starts_at, expires_on, is_active, is_expired, subscription_id, user_uuid, payment_method_id))

print(f"Payment: {len(payment_records)} synchronized records.")

# =====================================================
# Finalização: Commit e fechamento das conexões
# =====================================================
conn2.commit()   # Salva todas as alterações no banco de destino
cur1.close()     # Fecha cursor do banco de origem
cur2.close()     # Fecha cursor do banco de destino
conn1.close()    # Fecha conexão do banco de origem
conn2.close()    # Fecha conexão do banco de destino

print("\nSynchronization complete for all tables!")  # Mensagem final