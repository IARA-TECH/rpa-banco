import psycopg2
from dotenv import load_dotenv
from os import getenv
from datetime import datetime, timezone

load_dotenv()

# Conexão do banco de origem
conn1 = psycopg2.connect(
    dbname=getenv("DB_NAME_FIRST"),
    user=getenv("DB_USER"),
    password=getenv("DB_PASSWORD"),
    host=getenv("DB_HOST"),
    port=getenv("DB_PORT")
)
cur1 = conn1.cursor()

# Conexão do banco de destino
conn2 = psycopg2.connect(
    dbname=getenv("DB_NAME_SECOND"),
    user=getenv("DB_USER"),
    password=getenv("DB_PASSWORD"),
    host=getenv("DB_HOST"),
    port=getenv("DB_PORT")
)
cur2 = conn2.cursor()


cur1.execute("SELECT id, nome_unidade, cnpj_unidade, email_corporativo, status, descricao FROM fabrica;")
registros_fabrica = cur1.fetchall()

for id_fabrica, nome, cnpj, email, status, descricao in registros_fabrica:
    # Pega só o domínio
    domain = email.split('@')[1] if email and '@' in email else ''
    desativado = datetime.now() if not status else None

    cur2.execute("""
        INSERT INTO factory (pk_id, cnpj, name, domain, status, description)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (pk_id)
        DO UPDATE SET cnpj = EXCLUDED.cnpj,
                      name = EXCLUDED.name,
                      domain = EXCLUDED.domain,
                      deactivated_at = EXCLUDED.deactivated_at,
                      description = EXCLUDED.description;
    """, (id_fabrica, cnpj, nome, domain, desativado, descricao))

print(f"Fábrica: {len(registros_fabrica)} registros sincronizados")


# Tabela "Endereço"
cur1.execute("SELECT id, cep, estado, cidade, bairro, rua, numero, complemento, fk_fabrica FROM endereco;")
registros_endereco = cur1.fetchall()

for r in registros_endereco:
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

print(f"Endereço: {len(registros_endereco)} registros sincronizados")


# Tabela "Gênero"
cur1.execute("SELECT id, nome FROM genero;")
registros_genero = cur1.fetchall()

for r in registros_genero:
    cur2.execute("""
        INSERT INTO gender (pk_id, name)
        VALUES (%s, %s)
        ON CONFLICT (pk_id)
        DO UPDATE SET name = EXCLUDED.name;
    """, r)

print(f"Gênero: {len(registros_genero)} registros sincronizados")


# Tabela "Usuario"
cur1.execute("SELECT id, nome, email, senha, data_criacao, data_nascimento, status, fk_genero, id_gerente, fk_fabrica, tipo_acesso FROM usuario;")
registros_usuario = cur1.fetchall()

for id, nome, email, senha, data_criacao, data_nascimento, status, fk_genero, id_gerente, fk_fabrica in registros_usuario:
    desativado = datetime.now() if status != 'Ativo' else None

    data_criacao = datetime.combine(data_criacao, datetime.min.time(), tzinfo=timezone.utc)


    cur2.execute("""
        INSERT INTO user_account (pk_uuid, name, email, password, created_at, date_of_birth, deactivated_at, gender_id, user_manager_uuid, factory_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (pk_uuid)
        DO UPDATE SET name = EXCLUDED.name,
                      email = EXCLUDED.email,
                      password = EXCLUDED.password,
                      created_at = EXCLUDED.created_at,
                      birth_date = EXCLUDED.birth_date,
                      deactivated_at = EXCLUDED.deactivated_at,
                      gender_id = EXCLUDED.gender_id,
                      user_manager_uuid = EXCLUDED.user_manager_uuid,
                      factory_id = EXCLUDED.factory_id;
    """, (id, nome, email, senha, data_criacao, data_nascimento, desativado, fk_genero, id_gerente, fk_fabrica)
    )

print(f"Usuário: {len(registros_usuario)} registros sincronizados")



tipos_acesso_unicos = {(u[10], u[11]) for u in registros_usuario if u[10] and u[11]}

for tipo, descricao in tipos_acesso_unicos:
    cur2.execute("""
        INSERT INTO access_type (name, description)
        VALUES (%s, %s)
        ON CONFLICT (name)
            DO UPDATE SET description = EXCLUDED.description;
    """, (tipo, descricao))


print(f"Tipos de acesso: {len(tipos_acesso_unicos)} registros sincronizados")

cur2.execute("SELECT pk_id, name FROM access_type;")
mapa_tipos = {name: pk_id for pk_id, name in cur2.fetchall()}


for u in registros_usuario:
    id_usuario = u[0]
    tipo_acesso = u[10]

    if tipo_acesso in mapa_tipos:
        id_tipo = mapa_tipos[tipo_acesso]
        cur2.execute("""
            INSERT INTO user_access_type (user_account_uuid, access_type_id)
            VALUES (%s, %s)
            ON CONFLICT (user_account_uuid, access_type_id)
            DO UPDATE SET access_type_id = EXCLUDED.access_type_id;
        """, (id_usuario, id_tipo))

print("Relacionamentos usuário -> tipo de acesso sincronizados com sucesso.")



# Tabela "Plano"
cur1.execute("SELECT id, nome, valor, descricao, duracao FROM planos;")
registros_planos = cur1.fetchall()

for r in registros_planos:
    cur2.execute("""
        INSERT INTO subscription (pk_id, name, price, description, monthly_duration)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (pk_id)
        DO UPDATE SET name = EXCLUDED.name,
                      price = EXCLUDED.price,
                      description = EXCLUDED.description,
                      monthly_duration = EXCLUDED.monthly_duration;
    """, r)

print(f"Plano: {len(registros_planos)} registros sincronizados")



# Tabela "Método de Pagamento"
cur1.execute("SELECT id, tipo_pagamento FROM metodo_pagamento;")
registros_metodo_pagto = cur1.fetchall()

for r in registros_metodo_pagto:
    cur2.execute("""
        INSERT INTO payment_method (pk_id, name)
        VALUES (%s, %s)
        ON CONFLICT (pk_id)
        DO UPDATE SET name = EXCLUDED.name;
    """, r)

print(f"Método de Pagamento: {len(registros_metodo_pagto)} registros sincronizados")


# Tabela "Pagamento"
cur1.execute("SELECT id, total, data_pagamento, data_inicio, data_vencimento, status, fk_plano, fk_usuario, fk_metodo_pagamento FROM pagamento;")
registros_pagamento = cur1.fetchall()

for id, total, data_pagto, data_inicio, data_venc, status, fk_plano, fk_usuario, fk_metodo_pagto in registros_pagamento:
    expirado = False
    ativo = True
    
    if not status:
        expirado = True
        ativo = False

    cur2.execute("""
        INSERT INTO payment (pk_id, total, paid_at, starts_at, expires_on, is_active, is_expired, subscription_id, user_account_uuid, payment_method_id)
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
    """, (id, total, data_pagto, data_inicio, data_venc, ativo, expirado, fk_plano, fk_usuario, fk_metodo_pagto)
    )

print(f"Pagamento: {len(registros_pagamento)} registros sincronizados")




conn2.commit()
cur1.close()
cur2.close()
conn1.close()
conn2.close()

print("\nSincronização concluída para todas as tabelas!")