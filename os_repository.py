from datetime import datetime
from typing import Any, Dict, Optional, List, Tuple, Iterable
import json
from db_mysql import get_conn

def _parse_dt(s:str | None):
    if not s: return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None

def map_item(item: Dict[str, Any]) -> Tuple:
    cliente_rotulo = item.get("cliente")
    servico_rotulo = item.get("servico")
    status_servico = item.get("status_servico")
    endereco_instalacao_text = item.get("endereco_instalacao")
    pop = item.get("pop")
    id_tipo_ordem_servico = item.get("id_tipo_ordem_servico")
    descricao_abertura = item.get("descricao_abertura")
    descricao_servico  = item.get("descricao_servico")
    descricao_fechamento = item.get("descricao_fechamento")
    disponibilidade = item.get("disponibilidade")
    at = item.get("atendimento") or {}
    atendimento_protocolo = at.get("protocolo")
    atendimento_id = at.get("id_atendimento")
    atendimento_tipo = at.get("tipo_atendimento")
    atendimento_status = at.get("status_atendimento")
    tecnicos = item.get("tecnicos") or []
    if tecnicos and isinstance(tecnicos, list):
        t0 = tecnicos[0] or {}
        tecnico_principal_id = t0.get("id")
        tecnico_principal_nome = t0.get("name")
    else:
        tecnico_principal_id = None
        tecnico_principal_nome = None
    dados_cliente = item.get("dados_cliente") or {}
    tels = dados_cliente.get("telefones") or {}
    dados_servico = item.get("dados_servico") or {}
    end = item.get("dados_endereco_instalacao") or {}
    coords = (end.get("coordenadas") or {})
    assinatura = item.get("assinatura") or {}
    assinatura_assinado = assinatura.get("assinado")
    if isinstance(assinatura_assinado, bool):
        assinatura_assinado = 1 if assinatura_assinado else 0
    elif assinatura_assinado in (0,1,None):
        pass
    else:
        assinatura_assinado = None
    return (
        item.get("id_ordem_servico"),
        item.get("numero"),
        item.get("tipo"),
        item.get("status"),
        status_servico,
        id_tipo_ordem_servico,
        cliente_rotulo,
        servico_rotulo,
        endereco_instalacao_text,
        pop,
        descricao_abertura,
        descricao_servico,
        descricao_fechamento,
        disponibilidade,
        atendimento_protocolo,
        atendimento_id,
        atendimento_tipo,
        atendimento_status,
        tecnico_principal_id,
        tecnico_principal_nome,
        _parse_dt(item.get("data_cadastro")),
        _parse_dt(item.get("data_inicio_programado")),
        _parse_dt(item.get("data_termino_programado")),
        _parse_dt(item.get("data_inicio_executado")),
        _parse_dt(item.get("data_termino_executado")),
        dados_cliente.get("id_cliente"),
        dados_cliente.get("codigo_cliente"),
        dados_cliente.get("nome_razaosocial"),
        tels.get("telefone_primario"),
        tels.get("telefone_secundario"),
        dados_servico.get("id_cliente_servico"),
        dados_servico.get("descricao"),
        end.get("endereco"),
        end.get("numero"),
        end.get("bairro"),
        end.get("cidade"),
        end.get("estado"),
        end.get("cep"),
        coords.get("latitude"),
        coords.get("longitude"),
        assinatura_assinado,
        json.dumps(item, ensure_ascii=False)
    )

UPSERT_SQL = """
INSERT INTO ordens_servico (
  id_ordem_servico, numero, tipo, status,
  status_servico, id_tipo_ordem_servico, cliente_rotulo, servico_rotulo,
  endereco_instalacao_text, pop, descricao_abertura, descricao_servico, descricao_fechamento, disponibilidade,
  atendimento_protocolo, atendimento_id, atendimento_tipo, atendimento_status,
  tecnico_principal_id, tecnico_principal_nome,
  data_cadastro, data_inicio_programado, data_termino_programado,
  data_inicio_executado, data_termino_executado,
  cliente_id, cliente_codigo, cliente_nome,
  telefone_primario, telefone_secundario,
  id_cliente_servico, servico_descricao,
  endereco, numero_endereco, bairro, cidade, estado, cep, latitude, longitude,
  assinatura_assinado,
  raw, updated_at
) VALUES (
  %s,%s,%s,%s,
  %s,%s,%s,%s,
  %s,%s,%s,%s,%s,%s,
  %s,%s,%s,%s,
  %s,%s,
  %s,%s,%s,
  %s,%s,
  %s,%s,%s,
  %s,%s,
  %s,%s,
  %s,%s,%s,%s,%s,%s,%s,%s,
  %s,
  %s, NOW()
)
ON DUPLICATE KEY UPDATE
  numero = VALUES(numero),
  tipo   = VALUES(tipo),
  status = VALUES(status),
  status_servico = VALUES(status_servico),
  id_tipo_ordem_servico = VALUES(id_tipo_ordem_servico),
  cliente_rotulo = VALUES(cliente_rotulo),
  servico_rotulo = VALUES(servico_rotulo),
  endereco_instalacao_text = VALUES(endereco_instalacao_text),
  pop = VALUES(pop),
  descricao_abertura = VALUES(descricao_abertura),
  descricao_servico  = VALUES(descricao_servico),
  descricao_fechamento = VALUES(descricao_fechamento),
  disponibilidade = VALUES(disponibilidade),
  atendimento_protocolo = VALUES(atendimento_protocolo),
  atendimento_id = VALUES(atendimento_id),
  atendimento_tipo = VALUES(atendimento_tipo),
  atendimento_status = VALUES(atendimento_status),
  tecnico_principal_id = VALUES(tecnico_principal_id),
  tecnico_principal_nome = VALUES(tecnico_principal_nome),
  data_cadastro = VALUES(data_cadastro),
  data_inicio_programado = VALUES(data_inicio_programado),
  data_termino_programado = VALUES(data_termino_programado),
  data_inicio_executado  = VALUES(data_inicio_executado),
  data_termino_executado = VALUES(data_termino_executado),
  cliente_id = VALUES(cliente_id),
  cliente_codigo = VALUES(cliente_codigo),
  cliente_nome = VALUES(cliente_nome),
  telefone_primario = VALUES(telefone_primario),
  telefone_secundario = VALUES(telefone_secundario),
  id_cliente_servico = VALUES(id_cliente_servico),
  servico_descricao  = VALUES(servico_descricao),
  endereco = VALUES(endereco),
  numero_endereco = VALUES(numero_endereco),
  bairro = VALUES(bairro),
  cidade = VALUES(cidade),
  estado = VALUES(estado),
  cep = VALUES(cep),
  latitude = VALUES(latitude),
  longitude = VALUES(longitude),
  assinatura_assinado = VALUES(assinatura_assinado),
  raw = VALUES(raw),
  updated_at = NOW();
"""

def upsert_ordens(items: Iterable[Dict[str, Any]]) -> int:
    mapped: List[Tuple] = [map_item(x) for x in items]
    if not mapped: return 0
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.executemany(UPSERT_SQL, mapped)
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()

def _build_where(status: Optional[str], q: Optional[str], di: Optional[str], df: Optional[str]):
    where = []
    params: List[Any] = []
    if status:
        where.append("status LIKE %s")
        params.append(f"%{status}%")
    if di:
        where.append("(data_cadastro >= %s OR data_termino_executado >= %s)")
        params += [f"{di} 00:00:00", f"{di} 00:00:00"]
    if df:
        where.append("(data_cadastro <= %s OR data_termino_executado <= %s)")
        params += [f"{df} 23:59:59", f"{df} 23:59:59"]
    if q:
        where.append("(CAST(numero AS CHAR) LIKE %s OR tipo LIKE %s OR cliente_nome LIKE %s OR cidade LIKE %s OR cliente_rotulo LIKE %s OR servico_rotulo LIKE %s)")
        params += [f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%"]
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    return where_sql, params

def list_ordens(status: Optional[str], q: Optional[str], di: Optional[str], df: Optional[str],
                limit: int, offset: int) -> Dict[str, Any]:
    where_sql, params = _build_where(status, q, di, df)
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SELECT COUNT(*) AS total FROM ordens_servico {where_sql}", params)
        total = cur.fetchone()["total"]

        sql = f"""
          SELECT
            id_ordem_servico, numero, tipo, status, status_servico,
            data_cadastro, data_inicio_programado, data_termino_programado,
            data_inicio_executado, data_termino_executado,
            cliente_rotulo, cliente_nome, cidade, estado, servico_rotulo
          FROM ordens_servico
          {where_sql}
          ORDER BY COALESCE(data_termino_executado, data_cadastro) DESC
          LIMIT %s OFFSET %s
        """
        cur.execute(sql, params + [int(limit), int(offset)])
        rows = cur.fetchall()
        return {"items": rows, "total": total}
    finally:
        conn.close()

def get_ordem(id_os: int) -> Optional[Dict[str, Any]]:
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
          SELECT
            id_ordem_servico, numero, tipo, status, status_servico,
            data_cadastro, data_inicio_programado, data_termino_programado,
            data_inicio_executado, data_termino_executado,
            cliente_id, cliente_codigo, cliente_nome,
            telefone_primario, telefone_secundario,
            id_cliente_servico, servico_descricao,
            endereco, numero_endereco, bairro, cidade, estado, cep,
            latitude, longitude,
            cliente_rotulo, servico_rotulo, endereco_instalacao_text, pop,
            descricao_abertura, descricao_servico, descricao_fechamento, disponibilidade,
            atendimento_protocolo, atendimento_id, atendimento_tipo, atendimento_status,
            tecnico_principal_id, tecnico_principal_nome,
            assinatura_assinado,
            raw
          FROM ordens_servico
          WHERE id_ordem_servico = %s
        """, (id_os,))
        row = cur.fetchone()
        if not row: return None
        if isinstance(row.get("raw"), str):
            try: row["raw"] = json.loads(row["raw"])
            except Exception: pass
        return row
    finally:
        conn.close()

def list_concluidas_ontem() -> List[Dict[str, Any]]:
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
          SELECT
            id_ordem_servico, numero, tipo, status, status_servico,
            data_termino_executado, cliente_rotulo, cliente_nome, cidade, estado
          FROM ordens_servico
          WHERE status LIKE 'Finaliz%%'
            AND DATE(data_termino_executado) = DATE(DATE_SUB(CURDATE(), INTERVAL 1 DAY))
          ORDER BY data_termino_executado DESC
        """)
        return cur.fetchall()
    finally:
        conn.close()