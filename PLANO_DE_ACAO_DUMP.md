# Plano de Ação - Processo de Dump OpenEdge

Este arquivo serve como guia do processo de dump que precisamos reproduzir neste projeto e como controle do que já existe, do que está parcial e do que ainda falta implementar.

## Objetivo

Reproduzir o fluxo completo de dump do banco origem, usando comandos Progress/OpenEdge, persistindo logs e preparando a base para o futuro processo de load.

## Etapas do modelo atual

1. Verificar se o banco origem do dump está online ou offline.
2. Verificar se as pastas de destino do dump existem; se não existirem, criar.
3. Fazer dump da DF do banco origem para a pasta de destino com programa Progress.
4. Fazer dump das sequences do banco origem para a pasta de destino com programa Progress.
5. Fazer dump dos users do banco origem para a pasta de destino com programa Progress.
6. Fazer dump das rules de permissão do banco origem para a pasta de destino com programa Progress.
7. Inventariar todas as tabelas do banco origem com programa Progress.
8. Baseado no inventário, fazer um loop e fazer dump uma a uma das tabelas para a pasta de destino com uma linha de comando Progress.
9. Gerar o tabanalys inicial do banco origem com uma linha de comando Progress.

## Estado atual observado no projeto

### Feito

- A interface web já existe e permite configurar paths e selecionar bancos.
- A verificação de banco online/offline já existe via `proutil ... -C busy`.
- A criação de diretórios de destino já existe antes do dump da DF.
- O dump da DF já existe e é executado com programa Progress.
- Há persistência de estado e logs do job em `runtime/` e `logs/`.
- Existe um catálogo editável de comandos para as fases do dump.

### Parcial

- O fluxo de dump atual cobre apenas a DF por enquanto.
- O job já tem estrutura para acompanhar progresso por banco e por arquivo, mas ainda não cobre as etapas adicionais do dump completo.

### Não feito ainda

- Dump de sequences.
- Dump de users.
- Dump de rules de permissão.
- Inventário de tabelas.
- Loop de dump tabela a tabela baseado no inventário.
- Geração do tabanalys inicial.
- Fluxo de load.

## Ordem de implementação sugerida

1. Consolidar a checagem online/offline como pré-condição do job.
2. Garantir a criação da estrutura de diretórios para todas as saídas do dump.
3. Implementar e validar o dump da DF.
4. Implementar os dumps complementares: sequences, users e rules.
5. Implementar o inventário de tabelas.
6. Implementar o loop de dump por tabela.
7. Implementar o tabanalys inicial.
8. Só depois começar o desenho do load.

## Critério de pronto do dump

- O processo deve executar todas as etapas acima na ordem correta.
- Cada etapa deve registrar logs claros.
- O estado do job deve permitir saber o que foi concluído e o que falhou.
- O fluxo deve continuar organizado para facilitar a implementação do load depois.

## Observação

O processo de load será tratado em etapa própria, depois que o dump estiver fechado e validado.