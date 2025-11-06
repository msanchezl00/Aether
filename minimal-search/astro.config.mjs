import { defineConfig } from 'astro/config';
import node from '@astrojs/node'; // <-- Importa el adaptador

export default defineConfig({
  output: 'server', // <-- Asegúrate de que esto esté en 'server'
  adapter: node({   // <-- Añade el adaptador
    mode: 'standalone'
  }),
});