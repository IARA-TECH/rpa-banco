# RPA banco

Automação desenvolvida para **sincronizar dados entre dois bancos PostgreSQL** — o banco de origem (1º ano) e o banco de destino (2º ano) — garantindo a **consistência dos dados via UPSERT**.  
Registros que geram erro durante o processo são ignorados, permitindo que o script continue sem interrupções.

---

## 📚 Sumário

- [💡 Sobre o Projeto](#-sobre-o-projeto)
- [⚙️ Tecnologias Utilizadas](#️-tecnologias-utilizadas)
- [🧩 Como Executar](#-como-executar)
- [🔐 Variáveis de Ambiente](#-variáveis-de-ambiente)
- [📊 Estrutura de Sincronização](#-estrutura-de-sincronização)
- [👩‍💻 Autor](#-autor)

---

## 💡 Sobre o Projeto

Este script foi desenvolvido para **replicar e atualizar dados entre duas instâncias de banco de dados PostgreSQL**, normalmente representando **diferentes períodos operacionais (ex: 1º e 2º ano)**.

A automação garante que todas as tabelas sejam atualizadas com segurança, respeitando chaves primárias e relacionamentos entre entidades.

Principais recursos:
- Sincronização completa de tabelas relacionadas (usuários, endereços, pagamentos etc).
- Atualização incremental via **UPSERT (`ON CONFLICT DO UPDATE`)**.
- Tratamento de erros individualizado para evitar falhas globais.
- Log simplificado no terminal para cada etapa da sincronização.

---

## ⚙️ Tecnologias Utilizadas

| Categoria | Tecnologias |
| --- | --- |
| **Linguagem** | Python 3.8+ |
| **Banco de Dados** | PostgreSQL |
| **Bibliotecas Principais** | `psycopg2`, `python-dotenv`, `datetime`, `os` |
| **Ambiente** | `.env` para variáveis sensíveis |

---

## 🧩 Como Executar

### 1. Clonar o repositório
```bash
git clone https://github.com/IARA-TECH/rpa-banco.git
cd rpa-banco

```
### 2. Criar e ativar o ambiente virtual

```bash
python -m venv venv
# Linux/macOS
source venv/bin/activate
# Windows (PowerShell)
venv\Scripts\Activate.ps1
```

### 3. Instalar dependências
```bash
pip install -r requirements.txt

```
### 4. Definir as credenciais
```bash
DB_USER=usuario
DB_PASSWORD=senha
DB_HOST=localhost
DB_PORT=5432
DB_NAME_FIRST=banco_origem
DB_NAME_SECOND=banco_destino

```

### 5. Executar o script
```bash
python sincroniza_postgres.py

```
---

## 🔐 Variáveis de Ambiente (exemplo)

| Variável         | Descrição                                       |
| ---------------- | ----------------------------------------------- |
| `DB_USER`        | Usuário do banco PostgreSQL                     |
| `DB_PASSWORD`    | Senha do banco                                  |
| `DB_HOST`        | Host (geralmente `localhost` ou IP do servidor) |
| `DB_PORT`        | Porta (padrão: 5432)                            |
| `DB_NAME_FIRST`  | Nome do banco de origem                         |
| `DB_NAME_SECOND` | Nome do banco de destino                        |

---
## 📊 Estrutura de Sincronização

O script realiza a sincronização nas seguintes tabelas, nesta ordem:

1. factory — dados da fábrica (nome, CNPJ, status, descrição).
2. address — endereços vinculados à fábrica.
3. gender — gêneros de usuários.
4. user_account — contas de usuários, com mapeamento de fábrica e gênero.
5. access_type — tipos de acesso (Administrador, Supervisor, etc).
6. user_account_access_type — relacionamento entre usuários e tipos de acesso.
7. subscription — planos e assinaturas.
8. payment_method — métodos de pagamento.
9. payment — pagamentos realizados, vinculados ao usuário e plano.

Cada etapa é validada individualmente, e registros com erro são ignorados para manter a execução contínua.

---

## 👩‍💻 Autor

**IARA Tech**

Projeto interdisciplinar desenvolvido por alunos do Instituto J&F, como parte do ecossistema de soluções IARA, voltado à Automação desenvolvida para sincronização e integração de dados.

📍 São Paulo, Brasil  
📧 iaratech.oficial@gmail.com  
🌐 https://github.com/IARA-TECH


