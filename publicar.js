const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');

const sb = createClient(
  'https://ennsbpibfnuwlvtodukg.supabase.co',
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVubnNicGliZm51d2x2dG9kdWtnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDM4NDgzNywiZXhwIjoyMDg5OTYwODM3fQ.gnCLe-XvoWJoiVEG4jRCPCdX8OsevXACk0TISgo9S04'
);

const file = fs.readFileSync('./custo-servir/index.html');

sb.storage.from('dashboards')
  .upload('custo-servir/index.html', file, {
    upsert: true,
    contentType: 'text/html',
    cacheControl: '0'
  })
  .then(({ error }) => {
    if (error) console.error('ERRO:', error.message);
    else console.log('PUBLICADO com sucesso!');
  });
