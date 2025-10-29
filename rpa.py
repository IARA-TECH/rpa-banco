# =============================================
# Script de Sincronização entre dois bancos PostgreSQL
# Empresa: IARA
# Descrição: Copia e atualiza dados de várias tabelas do banco de origem (1º ano)
#            para o banco de destino (2º ano), mantendo consistência via UPSERT.
#            Caso algum registro gere erro, ele é ignorado e o script continua.
# =============================================

import psycopg2
from dotenv import load_dotenv
from os import getenv
from datetime import datetime, timezone

# =====================================================
# Carrega variáveis de ambiente (.env)
# =====================================================
load_dotenv()


def connect_db(db_name: str):
    """Cria uma conexão com um banco PostgreSQL usando variáveis de ambiente."""
    conn = psycopg2.connect(
        dbname=db_name,
        user=getenv("DB_USER"),
        password=getenv("DB_PASSWORD"),
        host=getenv("DB_HOST"),
        port=getenv("DB_PORT")
    )
    return conn, conn.cursor()


def safe_execute(cur, conn, query, params, context=""):
    """Executa uma query e ignora o erro se ocorrer."""
    try:
        cur.execute(query, params)
    except psycopg2.Error as e:
        conn.rollback()
        print(f"[IGNORED] {context} -> {e.pgerror.strip()}")
        return False
    return True


try:
    # =====================================================
    # Conexão com banco de origem e destino
    # =====================================================
    conn1, cur1 = connect_db(getenv("DB_NAME_FIRST"))
    conn2, cur2 = connect_db(getenv("DB_NAME_SECOND"))

    # =====================================================
    # Sincronização da tabela "Fábrica" (factory)
    # =====================================================
    cur1.execute("""
        SELECT id, nome_unidade, cnpj_unidade, email_corporativo, status, ramo
        FROM fabrica;
    """)
    factory_records = cur1.fetchall()
    ignored = 0

    for factory_id, name, cnpj, email, status, description in factory_records:
        domain = email.split('@')[1] if email and '@' in email else ''
        deactivated_at = datetime.now() if not status else None

        ok = safe_execute(cur2, conn2, """
            INSERT INTO factory (pk_id, cnpj, name, domain, deactivated_at, description)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (pk_id)
            DO UPDATE SET
                cnpj = EXCLUDED.cnpj,
                name = EXCLUDED.name,
                domain = EXCLUDED.domain,
                deactivated_at = EXCLUDED.deactivated_at,
                description = EXCLUDED.description;
        """, (factory_id, cnpj, name, domain, deactivated_at, description), f"Factory {factory_id}")

        if not ok:
            ignored += 1

    conn2.commit()
    print(f"Factory: {len(factory_records) - ignored} synchronized, {ignored} ignored.")

    # =====================================================
    # Sincronização da tabela "Endereço" (address)
    # =====================================================
    cur1.execute("""
        SELECT id, cep, estado, cidade, bairro, rua, numero, complemento, fk_fabrica
        FROM endereco;
    """)
    address_records = cur1.fetchall()
    ignored = 0

    for r in address_records:
        ok = safe_execute(cur2, conn2, """
            INSERT INTO address (pk_id, cep, state, city, neighborhood, street,
                                 building_number, complement, factory_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (pk_id)
            DO UPDATE SET
                cep = EXCLUDED.cep,
                state = EXCLUDED.state,
                city = EXCLUDED.city,
                neighborhood = EXCLUDED.neighborhood,
                street = EXCLUDED.street,
                building_number = EXCLUDED.building_number,
                complement = EXCLUDED.complement,
                factory_id = EXCLUDED.factory_id;
        """, r, f"Address {r[0]}")
        if not ok:
            ignored += 1

    conn2.commit()
    print(f"Address: {len(address_records) - ignored} synchronized, {ignored} ignored.")

    # =====================================================
    # Sincronização da tabela "Gênero" (gender)
    # =====================================================
    cur1.execute("SELECT DISTINCT genero FROM usuario WHERE genero IS NOT NULL;")
    gender_records = cur1.fetchall()
    gender_map = {}
    ignored = 0

    for i, (gender_name,) in enumerate(gender_records, start=1):
        gender_map[gender_name] = i
        if gender_name == 'masc':
            gender_name = 'Masculino'
            i = 2
        elif gender_name == 'fem':
            gender_name = 'Feminino'
            i = 1
        else:
            gender_name = 'Outro'
            i = 3

        ok = safe_execute(cur2, conn2, """
            INSERT INTO gender (pk_id, name)
            VALUES (%s, %s)
            ON CONFLICT (pk_id)
            DO UPDATE SET name = EXCLUDED.name;
        """, (i, gender_name), f"Gender {i}")
        if not ok:
            ignored += 1

    conn2.commit()
    print(f"Gender: {len(gender_records) - ignored} synchronized, {ignored} ignored.")

    # =====================================================
    # Sincronização da tabela "Usuário" (user_account)
    # =====================================================
    cur1.execute("""
        SELECT id, nome, email, senha, data_criacao, data_nascimento, status,
               genero, id_gerente, fk_fabrica, tipo_acesso, desc_tipoacesso
        FROM usuario;
    """)
    user_account_records = cur1.fetchall()
    ignored = 0

    for r in user_account_records:
        deactivated_at = datetime.now() if r[6] != 'Ativo' else None
        created_at = datetime.combine(r[4], datetime.min.time(), tzinfo=timezone.utc)
        manager_uuid = r[8] if r[8] else None
        gender_id = gender_map.get(r[7])

        ok = safe_execute(cur2, conn2, """
            INSERT INTO user_account (pk_uuid, name, email, password, created_at,
                                      date_of_birth, deactivated_at, gender_id,
                                      user_manager_uuid, factory_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (pk_uuid)
            DO UPDATE SET
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                password = EXCLUDED.password,
                created_at = EXCLUDED.created_at,
                date_of_birth = EXCLUDED.date_of_birth,
                deactivated_at = EXCLUDED.deactivated_at,
                gender_id = EXCLUDED.gender_id,
                user_manager_uuid = EXCLUDED.user_manager_uuid,
                factory_id = EXCLUDED.factory_id;
        """, (r[0], r[1], r[2], r[3], created_at, r[5], deactivated_at, gender_id, manager_uuid, r[9]), f"User {r[0]}")
        if not ok:
            ignored += 1

    conn2.commit()
    print(f"User Account: {len(user_account_records) - ignored} synchronized, {ignored} ignored.")

    # =====================================================
    # Sincronização da tabela "Tipo de Acesso" (access_type)
    # =====================================================
    access_type_records = {(u[10], u[11]) for u in user_account_records if u[10] and u[11]}
    ignored = 0

    for type, description in access_type_records:
        if type == 1:
            type = 'Administrador'
            description = 'Pode criar cargos, realizar pagamentos e visualizar informações sensíveis sobre a fábrica'
        elif type == 2:
            type = 'Supervisor'
            description = 'Pode revisar e aprovar solicitações de alterações nos relatórios'
        elif type == 3:
            type = 'Solicitante'
            description = 'Pode solicitar alterações nos relatórios.'
        else:
            type = 'Visualizador'
            description = 'Pode visualizar relatórios e informações do sistema.'

        ok = safe_execute(cur2, conn2, """
            INSERT INTO access_type (name, description)
            VALUES (%s, %s)
            ON CONFLICT (name)
            DO UPDATE SET description = EXCLUDED.description;
        """, (type, description), f"AccessType {type}")
        if not ok:
            ignored += 1

    conn2.commit()
    print(f"Access Type: {len(access_type_records) - ignored} synchronized, {ignored} ignored.")

    # =====================================================
    # Relaciona "Usuários" e "Tipos de Acesso"
    # =====================================================
    cur2.execute("SELECT pk_id, name FROM access_type;")
    map_types = {name: pk_id for pk_id, name in cur2.fetchall()}
    ignored = 0

    for u in user_account_records:
        user_uuid = u[0]
        tipo = u[10]

        access_type_name = (
            'Administrador' if tipo == 1 else
            'Supervisor' if tipo == 2 else
            'Solicitante' if tipo == 3 else
            'Visualizador'
        )

        if access_type_name in map_types:
            access_type_id = map_types[access_type_name]
            ok = safe_execute(cur2, conn2, """
                INSERT INTO user_account_access_type (user_account_uuid, access_type_id)
                VALUES (%s, %s)
                ON CONFLICT (user_account_uuid, access_type_id)
                DO NOTHING;
            """, (user_uuid, access_type_id), f"UserAccess {user_uuid}")
            if not ok:
                ignored += 1

    conn2.commit()
    print(f"User -> AccessType relations synchronized, {ignored} ignored.")

    # =====================================================
    # Sincronização da tabela "Plano" (subscription)
    # =====================================================
    cur1.execute("SELECT id, nome, valor, descricao, duracao FROM plano;")
    registros_planos = cur1.fetchall()
    ignored = 0

    for r in registros_planos:
        delta = r[4]
        total_days = delta.days
        r = list(r)
        r[4] = total_days // 30

        ok = safe_execute(cur2, conn2, """
            INSERT INTO subscription (pk_id, name, price, description, monthly_duration)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (pk_id)
            DO UPDATE SET
                name = EXCLUDED.name,
                price = EXCLUDED.price,
                description = EXCLUDED.description,
                monthly_duration = EXCLUDED.monthly_duration;
        """, r, f"Subscription {r[0]}")
        if not ok:
            ignored += 1

    conn2.commit()
    print(f"Subscription: {len(registros_planos) - ignored} synchronized, {ignored} ignored.")

    # =====================================================
    # Sincronização da tabela "Método de Pagamento" (payment_method)
    # =====================================================
    cur1.execute("SELECT id, tipo_pagamento FROM metodo_pagamento;")
    payment_method_records = cur1.fetchall()
    ignored = 0

    for r in payment_method_records:
        ok = safe_execute(cur2, conn2, """
            INSERT INTO payment_method (pk_id, name)
            VALUES (%s, %s)
            ON CONFLICT (pk_id)
            DO UPDATE SET name = EXCLUDED.name;
        """, r, f"PaymentMethod {r[0]}")
        if not ok:
            ignored += 1

    conn2.commit()
    print(f"Payment Method: {len(payment_method_records) - ignored} synchronized, {ignored} ignored.")

    # =====================================================
    # Sincronização da tabela "Pagamento" (payment)
    # =====================================================
    cur1.execute("""
        SELECT id, valor, data_pagamento, data_inicio, data_vencimento, status,
               fk_plano, fk_fabrica, fk_metodopag
        FROM pagamento;
    """)
    payment_records = cur1.fetchall()
    ignored = 0

    for id, total, paid_at, starts_at, expires_on, status, subscription_id, factory_id, payment_method_id in payment_records:
        is_expired = not status
        is_active = status

        cur2.execute("""
            SELECT us.pk_uuid
            FROM user_account us
            JOIN user_account_access_type ua ON ua.user_account_uuid = us.pk_uuid
            JOIN access_type ac ON ac.pk_id = ua.access_type_id
            WHERE us.factory_id = %s
            AND ac.pk_id = (SELECT MIN(pk_id) FROM access_type)
            LIMIT 1;
        """, (factory_id,))
        result = cur2.fetchone()
        user_uuid = result[0] if result else None

        if not user_uuid:
            continue

        ok = safe_execute(cur2, conn2, """
            INSERT INTO payment (pk_id, total, paid_at, starts_at, expires_on,
                                 is_active, is_expired, subscription_id,
                                 user_account_uuid, payment_method_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (pk_id)
            DO UPDATE SET
                total = EXCLUDED.total,
                paid_at = EXCLUDED.paid_at,
                starts_at = EXCLUDED.starts_at,
                expires_on = EXCLUDED.expires_on,
                is_active = EXCLUDED.is_active,
                is_expired = EXCLUDED.is_expired,
                subscription_id = EXCLUDED.subscription_id,
                user_account_uuid = EXCLUDED.user_account_uuid,
                payment_method_id = EXCLUDED.payment_method_id;
        """, (id, total, paid_at, starts_at, expires_on, is_active, is_expired,
              subscription_id, user_uuid, payment_method_id), f"Payment {id}")
        if not ok:
            ignored += 1

    conn2.commit()
    print(f"Payment: {len(payment_records) - ignored} synchronized, {ignored} ignored.")
    print("\nSynchronization complete for all tables!")

except Exception as e:
    conn2.rollback()
    print("\nCritical error during synchronization:", e)

finally:
    if 'cur1' in locals(): cur1.close()
    if 'cur2' in locals(): cur2.close()
    if 'conn1' in locals(): conn1.close()
    if 'conn2' in locals(): conn2.close()
