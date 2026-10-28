// Nest (modo monorepo) compila cada app con webpack: empaqueta el código de la app
// y de libs/ en un único dist/apps/<app>/main.js y deja node_modules como externos.
// Se quita el chequeo de tipos en paralelo para que el build de Docker sea rápido;
// los tipos se verifican con `npm run typecheck`.
module.exports = function (options) {
  return {
    ...options,
    plugins: (options.plugins || []).filter(
      (plugin) => plugin?.constructor?.name !== 'ForkTsCheckerWebpackPlugin',
    ),
  };
};
