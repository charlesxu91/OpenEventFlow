# @openeventflow/react

React bindings for OpenEventFlow.

The package exports a factory so React remains a peer dependency:

```js
const { createOpenEventFlowReact } = require("@openeventflow/react");
const { OpenEventFlowProvider, useAnalytics, useScreen } = createOpenEventFlowReact(React);
```
