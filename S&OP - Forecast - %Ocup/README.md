# Publicador do Simulador S&OP — Como usar

Este pacote publica a página do **Simulador S&OP / Forecast** no GitHub da Torre.
Tudo automático: clona o repositório, copia os arquivos, atualiza o painel
principal com o novo card e faz o push.

---

## O que tem nesta pasta

| Arquivo | O que é |
|---------|---------|
| `PUBLICAR.bat` | **Duplo-clique aqui** se você está no Windows |
| `PUBLICAR.command` | **Duplo-clique aqui** se você está no Mac |
| `publicar_simulador.py` | Cérebro do script (não mexa) |
| `index.html` | Página do simulador a ser publicada |
| `README.md` | Este arquivo |

---

## Antes de usar pela primeira vez

### 1. Tenha o Git instalado

- **Windows:** baixe em <https://git-scm.com/download/win> → instale com as
  opções padrão (basta apertar "Next" em tudo).
- **Mac:** abra o Terminal e digite `git --version`. Se pedir para instalar,
  aceite.

### 2. Tenha o Python instalado

- **Windows:** abra a Microsoft Store, busque "Python 3.12" e clique em "Obter".
  É grátis.
- **Mac:** já vem instalado. Se aparecer erro, instale com Homebrew
  (`brew install python3`).

### 3. Saiba a URL do seu repositório

A URL fica na barra de endereço quando você abre o repo no GitHub. Termina em
`.git`. Por exemplo:

```
https://github.com/seuusuario/torre-controle.git
```

> 💡 Você só precisa colar uma vez — o script lembra para as próximas execuções.

---

## Como usar

1. **Coloque os 4 arquivos numa pasta qualquer** do seu computador.
2. **Duplo-clique** em `PUBLICAR.bat` (Windows) ou `PUBLICAR.command` (Mac).
3. **Responda as perguntas** que aparecerem:
   - Na primeira vez, vai pedir a URL do repositório.
   - Nas próximas vezes, é só apertar Enter para confirmar.
4. **Pronto!** O script vai imprimir "TUDO PUBLICADO COM SUCESSO" no fim.
   Acesse a Torre e o card **"S&OP - Forecast | % Ocupação"** já vai estar lá.

---

## Quando devo rodar de novo?

- Sempre que **atualizar a página `index.html`** com uma versão nova.
- Sempre que o Claude (ou outro programador) enviar um arquivo `index.html`
  atualizado. Você substitui o arquivo na pasta e roda o script de novo.

O script é **idempotente** — pode rodar quantas vezes quiser, ele não duplica
nada. Só atualiza o que mudou.

---

## Problemas comuns

### "Git não está instalado"
Volte ao passo 1 da seção *Antes de usar*.

### "Python não está instalado"
Volte ao passo 2 da seção *Antes de usar*.

### "Pediu senha do GitHub e não funcionou"
GitHub não aceita mais senha de conta. Você precisa de um **Personal Access
Token**:

1. Vá em <https://github.com/settings/tokens/new>
2. Dê um nome (ex: "publicador-torre")
3. Marque a permissão **"repo"**
4. Clique em "Generate token" e **copie o token** (você só vê uma vez!)
5. Quando o script pedir senha, cole o token no lugar da senha.

### "Falha no push"
Provavelmente alguém atualizou o repo enquanto você não rodava. Apague a pasta
`_torre_repo/` que aparece junto com o script e rode de novo.

### "O card não apareceu na Torre"
- Se a Torre está no **GitHub Pages**, pode levar até 2 minutos.
- Atualize o navegador apertando **Ctrl+F5** (Windows) ou **Cmd+Shift+R** (Mac).
- Se ainda não aparece, verifique no GitHub se o último commit foi feito.

---

## Dúvidas?

O script imprime tudo o que está fazendo em tempo real, então qualquer erro
fica claro na tela. Se aparecer mensagem em vermelho com "Erro", mande print
para quem te enviou este pacote.
