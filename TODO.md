**TRATAMENTO DAS COLUNAS DE CID's PARA O SUBSISTEMA SIM**
- As colunas: `linhaa`, `linhab`, `linhac`, `linhad`, `linhaii` contém CID's, os quais deveriam estar em um dos 2 formatos aceitos:
  - LNN (1 letra + 2 números): Exemplo: A01
  - LNNN (1 letra + 3 números): Exemplo: J123
- Porém essas colunas apresentam strings de CID's com 2 problemas gerais:
  - Muitos registros iniciam a string do CID com um asterísco. Exemplo: *A01. Esse asterísco inicial tem que ser removido da string antes de ser validado o formato do CID.
  - Alguns poucos registros contém mais de 1 CID registrado na mesma célula da tabela, os quais estão separados por um segundo asteísco. 
    - Exemplo de 2 CID's na mesma célula: *A01*J128
    - Exemplo de 4 CID's na mesma célula: *I251*N19X*E149*I10X
- Para resolver esse problema sem perder informação, a tabela de dados RAW do SIM para essas 5 colunas específicas deve ter o tipo de uma lista de strings. Desse modo, será possível extrair todos os CID's dos registros de acordo com o separados 'asterísco' e depois cada registro será uma lista/arrays de CID's.

**Parse de colunas de data com formato específico**
- As colunas `dtobito`, `dtnasc`, `dtinvestig`, `dtatestado`, `dtcadastro`, `dtrecebim`, `dtrecoriga`, `dtcadinv`, `dtconinv`, `dtcadinf`, `dtconcaso` estão armazenando uma data, porém em 2 formato específicos que dificultam o parse natuarl:
  - Formato 1: `ddmmyyyy`. Exemplo: 31122023 -> '2023-12-31'
  - Formato 2: `dmmyyyy`. Exemplo: 1012023 -> '2023-01-01'.
    - Veja que nesse formato 2, caso o primeiro digito do dia seja '0', esse primeiro dígito não aparece na string original do dado e deve ser deduzido.