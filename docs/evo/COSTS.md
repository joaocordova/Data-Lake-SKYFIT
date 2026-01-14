# Análise de Custos e Arquitetura - Skyfit Data Lake

##  Volume de Dados Estimado

| Entidade | Registros | Tamanho Bronze | Tamanho STG | Tamanho CORE |
|----------|-----------|----------------|-------------|--------------|
| Entries | **110M** | ~50 GB | ~80 GB | ~40 GB |
| Sales + Items + Receivables | 6.7M + 10M + 15M | ~15 GB | ~25 GB | ~15 GB |
| Members + Memberships | 1.3M + 3M | ~3 GB | ~5 GB | ~3 GB |
| Prospects | 612K | ~0.5 GB | ~1 GB | ~0.5 GB |
| **TOTAL** | **~145M** | **~70 GB** | **~110 GB** | **~60 GB** |

---

##  Custos Estimados Azure (Mensal)

### 1. Azure Data Lake Storage (ADLS Gen2)

| Tier | Custo/GB | Volume | Custo Mensal |
|------|----------|--------|--------------|
| Hot | $0.0184/GB | 70 GB | **$1.30** |
| Cool | $0.01/GB | 70 GB | $0.70 |

**Operações (estimativa)**:
- Write: ~$0.05 per 10k operations → ~$5
- Read: ~$0.01 per 10k operations → ~$2

**ADLS Total: ~$8-10/mês**

### 2. PostgreSQL Flexible Server

Configuração atual (visible no screenshot):
- **Tier**: Burstable B2ms (2 vCores, 8 GB RAM)
- **Storage**: 128 GB
- **Region**: East US 2

| Item | Custo Mensal |
|------|--------------|
| Compute (B2ms) | ~$50-60 |
| Storage (128 GB) | ~$16 |
| Backup | ~$5 |
| **Total PostgreSQL** | **~$70-80/mês** |

### 3. Custo Total Infraestrutura

| Componente | Custo Mensal |
|------------|--------------|
| ADLS Gen2 | ~$10 |
| PostgreSQL Flexible | ~$75 |
| **TOTAL** | **~$85/mês** |

---

##  PostgreSQL para 145M registros - Faz sentido?

###  PRÓS

1. **Performance OK com particionamento**
   - Entries particionado por ano (7 partições)
   - Cada partição ~16M registros - gerenciável

2. **Custo baixo**
   - $75/mês vs $500+ para soluções analíticas dedicadas

3. **Simplicidade operacional**
   - Já está configurado
   - Familiar para a equipe
   - Backup automático Azure

4. **Queries OLTP funcionam bem**
   - Buscar frequência de 1 membro
   - Últimas vendas de 1 cliente
   - Dados de 1 contrato

###  LIMITAÇÕES

1. **Queries analíticas pesadas**
   - Aggregations em 110M entries = lento (30s+)
   - Full table scans = problemático
   - Joins grandes = memory pressure

2. **Concorrência**
   - 2 vCores limita queries simultâneas
   - Loads pesados bloqueiam reads

3. **Escala futura**
   - Se entries crescer 2x/ano → problema em 2 anos

---

##  Recomendações por Cenário

### Cenário 1: Manter PostgreSQL (Custo: $85/mês)
**Quando usar**: Consultas pontuais, relatórios simples, MVP

```
 Recomendado se:
- Queries são principalmente por membro/venda específica
- Relatórios pré-agregados são suficientes
- Budget é limitado
- Equipe pequena

 Mitigações necessárias:
- Criar views materializadas para métricas comuns
- Índices otimizados
- Particionar tabelas grandes
- Evitar full scans na camada CORE
```

### Cenário 2: PostgreSQL + Read Replica (Custo: ~$150/mês)
**Quando usar**: Separar workloads OLTP/OLAP

```
✅ Vantagens:
- Analytics na replica (não afeta produção)
- Pode fazer queries pesadas
- Failover automático

Configuração:
- Primary: Loads e writes
- Replica: Queries analíticas
```

### Cenário 3: Hybrid - PostgreSQL (CORE) + Azure Synapse/Databricks (Custo: ~$300-500/mês)
**Quando usar**: Analytics pesados, ML, BI avançado

```
 Vantagens:
- Melhor performance para big data
- Escala horizontal
- Integração com Power BI

Arquitetura:
Bronze (ADLS) → PostgreSQL (CORE/Operacional)
                   ↓
              Synapse/Databricks (Analytics)
```

---

##  Recomendação Final

### Para o momento atual:

**Manter PostgreSQL com otimizações** (Cenário 1)

Justificativa:
1. 145M registros ainda é gerenciável
2. Custo baixo ($85/mês)
3. Não há demanda clara por analytics complexo ainda
4. Pode evoluir conforme necessidade

### Otimizações a implementar:

1. **Views Materializadas** para métricas frequentes:
```sql
-- Refresh diário
CREATE MATERIALIZED VIEW analytics.mv_daily_entries AS
SELECT DATE(entry_date), branch_id, COUNT(*)
FROM core.evo_entries
GROUP BY 1, 2;
```

2. **Particionamento** (já implementado para entries)

3. **Índices parciais**:
```sql
CREATE INDEX idx_entries_recent ON core.evo_entries(entry_date)
WHERE entry_date >= CURRENT_DATE - INTERVAL '90 days';
```

4. **Limitar queries na aplicação**:
```sql
-- Sempre com filtro de data
WHERE entry_date >= CURRENT_DATE - INTERVAL '30 days'
```

---

##  Gatilhos para Mudar de Arquitetura

Considerar Cenário 2 ou 3 quando:

- [ ] Entries ultrapassar 200M registros
- [ ] Queries analíticas frequentes (>10/dia)
- [ ] Tempo de resposta >30s inaceitável
- [ ] Necessidade de ML/BI avançado
- [ ] Time de dados crescer (>3 pessoas)

---

##  Resumo de Custos

| Cenário | Custo/mês | Complexidade | Quando usar |
|---------|-----------|--------------|-------------|
| **PostgreSQL atual** | $85 | Baixa | ✅ Agora |
| PostgreSQL + Replica | $150 | Média | Se analytics crescer |
| PostgreSQL + Synapse | $400+ | Alta | Big Data/ML |

**Recomendação**: Começar com Cenário 1, evoluir conforme demanda.
