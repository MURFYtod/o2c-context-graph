const graphContainer = document.getElementById("graph");
const nodeMeta = document.getElementById("nodeMeta");
const reloadBtn = document.getElementById("reloadBtn");
const askBtn = document.getElementById("askBtn");
const questionEl = document.getElementById("question");
const chatLog = document.getElementById("chatLog");

let network;

function showNodeMetadata(node) {
  if (!node) {
    nodeMeta.textContent = "No node selected.";
    return;
  }
  const payload = node.raw || {
    id: node.id,
    label: node.label,
  };
  nodeMeta.textContent = JSON.stringify(payload, null, 2);
}

function appendMessage(text, type) {
  const div = document.createElement("div");
  div.className = type === "user" ? "msg-user" : "msg-ai";
  div.textContent = text;
  chatLog.appendChild(div);
  chatLog.scrollTop = chatLog.scrollHeight;
}

function appendAiResponse(data) {
  const wrapper = document.createElement("div");
  wrapper.className = "msg-ai";

  const answer = document.createElement("div");
  answer.className = "msg-title";
  answer.textContent = data.answer || "No answer.";
  wrapper.appendChild(answer);

  if (data.rows && data.rows.length) {
    const table = document.createElement("table");
    table.className = "result-table";

    const header = document.createElement("tr");
    const cols = data.columns && data.columns.length ? data.columns : [];
    cols.forEach((c) => {
      const th = document.createElement("th");
      th.textContent = c;
      header.appendChild(th);
    });
    if (cols.length) {
      table.appendChild(header);
    }

    data.rows.slice(0, 5).forEach((row) => {
      const tr = document.createElement("tr");
      row.forEach((cell) => {
        const td = document.createElement("td");
        td.textContent = cell === null ? "-" : String(cell);
        tr.appendChild(td);
      });
      table.appendChild(tr);
    });
    wrapper.appendChild(table);
  }

  if (data.sql) {
    const sql = document.createElement("details");
    const summary = document.createElement("summary");
    summary.textContent = "Show generated SQL";
    const code = document.createElement("pre");
    code.className = "sql-block";
    code.textContent = data.sql;
    sql.appendChild(summary);
    sql.appendChild(code);
    wrapper.appendChild(sql);
  }

  chatLog.appendChild(wrapper);
  chatLog.scrollTop = chatLog.scrollHeight;
}

async function loadGraph() {
  const resp = await fetch("/api/graph");
  const data = await resp.json();

  const nodes = data.nodes.map((n) => ({
    id: n.id,
    label: `${n.entity}\n${n.entity_id}`,
    shape: "dot",
    size: 8,
    color: "#76b3e0",
    raw: n,
  }));
  const edges = data.edges.map((e) => ({
    from: e.source,
    to: e.target,
    label: e.relation || "",
    arrows: "to",
    color: { color: "#c2d9ee" },
  }));
  const dsNodes = new vis.DataSet(nodes);
  const dsEdges = new vis.DataSet(edges);

  if (network) {
    network.destroy();
  }

  network = new vis.Network(
    graphContainer,
    { nodes: dsNodes, edges: dsEdges },
    {
      layout: { improvedLayout: true },
      physics: { stabilization: false, barnesHut: { gravitationalConstant: -1800 } },
      edges: { font: { size: 8, align: "middle" }, smooth: false },
      interaction: { hover: true, multiselect: false },
    }
  );

  network.on("click", (params) => {
    if (!params.nodes.length) {
      nodeMeta.textContent = "Click a node to inspect metadata.";
      return;
    }
    const node = dsNodes.get(params.nodes[0]);
    showNodeMetadata(node);
  });

  network.on("selectNode", (params) => {
    const node = dsNodes.get(params.nodes[0]);
    showNodeMetadata(node);
  });
}

async function ask() {
  const question = questionEl.value.trim();
  if (!question) return;
  appendMessage(question, "user");
  questionEl.value = "";
  const resp = await fetch("/api/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question }),
  });
  const data = await resp.json();
  appendAiResponse(data);
}

reloadBtn.addEventListener("click", async () => {
  await fetch("/api/reload", { method: "POST" });
  await loadGraph();
  appendMessage("Dataset reloaded.", "ai");
});
askBtn.addEventListener("click", ask);
questionEl.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    ask();
  }
});

appendMessage("Hi! I can help you analyze the Order to Cash dataset. Ask me about orders, deliveries, billing, payments, and flow breaks.", "ai");
loadGraph();
