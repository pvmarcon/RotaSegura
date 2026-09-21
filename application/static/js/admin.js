(() => {
  const qrGeneratorForm = document.getElementById("qr-generator-form");
  const qrStudentId = document.getElementById("qr-student-id");
  const qrValidity = document.getElementById("qr-validity");
  const qrPreview = document.getElementById("qr-code-preview");
  const qrOutputLabel = document.getElementById("qr-output-label");
  const qrToken = document.getElementById("qr-token");
  const qrCopyToken = document.getElementById("qr-copy-token");
  const qrGeneratorStatus = document.getElementById("qr-generator-status");

  if (qrGeneratorForm) {
    qrGeneratorForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const alunoId = qrStudentId.value.trim();
      const validadeSegundos = (parseInt(qrValidity.value, 10) || 5) * 60;
      qrGeneratorStatus.textContent = "Gerando...";
      qrGeneratorStatus.className = "qr-generator-status";

      let response;
      try {
        response = await fetch("/admin/gerar-qr", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ aluno_id: alunoId, validade_segundos: validadeSegundos }),
        });
      } catch (error) {
        qrGeneratorStatus.textContent = "Falha de rede ao gerar o QR Code.";
        qrGeneratorStatus.className = "qr-generator-status error";
        return;
      }

      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        qrGeneratorStatus.textContent = data.detail || "Não foi possível gerar o QR Code.";
        qrGeneratorStatus.className = "qr-generator-status error";
        return;
      }

      qrPreview.replaceChildren();
      new QRCode(qrPreview, { text: data.token, width: 190, height: 190, colorDark: "#111113", colorLight: "#ffffff" });
      qrOutputLabel.textContent = `QR Code válido por ${qrValidity.value} minuto(s) para ${data.aluno_id}`;
      qrToken.value = data.token;
      qrCopyToken.disabled = false;
      qrGeneratorStatus.textContent = "QR Code gerado com assinatura HMAC-SHA256.";
      qrGeneratorStatus.className = "qr-generator-status success";
    });

    qrCopyToken.addEventListener("click", async () => {
      if (!qrToken.value) return;
      await navigator.clipboard.writeText(qrToken.value);
      qrGeneratorStatus.textContent = "Token copiado.";
      qrGeneratorStatus.className = "qr-generator-status success";
    });
  }

  const triggerBtn = document.getElementById("trigger-btn");
  const minCountInput = document.getElementById("min-count");
  const maxCountInput = document.getElementById("max-count");
  const modal = document.getElementById("confirm-modal");
  const confirmCancel = document.getElementById("confirm-cancel");
  const confirmSubmit = document.getElementById("confirm-submit");
  const confirmPasswordInput = document.getElementById("confirm-password");

  const statSent = document.getElementById("stat-sent");
  const statSuccess = document.getElementById("stat-success");
  const statFail = document.getElementById("stat-fail");
  const statusDot = document.getElementById("sim-status-dot");
  const statusText = document.getElementById("sim-status-text");
  const chartMaxLabel = document.getElementById("chart-max-label");
  const canvas = document.getElementById("activity-chart");
  const ctx = canvas.getContext("2d");

  // Mantenha em sincronia com --accent-glow no admin.css
  const DATA_COLOR = "99, 102, 241";

  let runningJobId = null;
  let eventSource = null;
  let finishingJobId = null;

  function resizeCanvas() {
    const ratio = window.devicePixelRatio || 1;
    canvas.width = canvas.clientWidth * ratio;
    canvas.height = canvas.clientHeight * ratio;
    ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  }
  window.addEventListener("resize", resizeCanvas);
  resizeCanvas();

  function drawLineChart(buckets) {
    const w = canvas.clientWidth;
    const h = canvas.clientHeight;
    ctx.clearRect(0, 0, w, h);

    const max = Math.max(...buckets, 1);
    chartMaxLabel.textContent = max;

    const stepX = w / (buckets.length - 1);
    const points = buckets.map((v, i) => ({
      x: i * stepX,
      y: h - (v / max) * (h - 8) - 4,
    }));

    // Curva suavizada usando pontos médios entre cada par (técnica padrão em canvas)
    ctx.beginPath();
    ctx.moveTo(points[0].x, points[0].y);
    for (let i = 1; i < points.length - 1; i++) {
      const midX = (points[i].x + points[i + 1].x) / 2;
      const midY = (points[i].y + points[i + 1].y) / 2;
      ctx.quadraticCurveTo(points[i].x, points[i].y, midX, midY);
    }
    const last = points[points.length - 1];
    ctx.quadraticCurveTo(last.x, last.y, last.x, last.y);

    ctx.strokeStyle = `rgb(${DATA_COLOR})`;
    ctx.lineWidth = 2;
    ctx.lineJoin = "round";
    ctx.stroke();

    // Fecha o caminho na base pra preencher a área com um gradiente sutil
    ctx.lineTo(last.x, h);
    ctx.lineTo(points[0].x, h);
    ctx.closePath();

    const gradient = ctx.createLinearGradient(0, 0, 0, h);
    gradient.addColorStop(0, `rgba(${DATA_COLOR}, 0.25)`);
    gradient.addColorStop(1, `rgba(${DATA_COLOR}, 0)`);
    ctx.fillStyle = gradient;
    ctx.fill();

    // Marcador no ponto mais recente (agora), como nos gráficos de referência
    ctx.beginPath();
    ctx.arc(last.x, last.y, 3, 0, Math.PI * 2);
    ctx.fillStyle = `rgb(${DATA_COLOR})`;
    ctx.fill();
  }

  function setStatus(text, dotState) {
    statusText.textContent = text;
    statusDot.className = "status-dot" + (dotState ? ` ${dotState}` : "");
  }

  function openModal() {
    modal.hidden = false;
    confirmPasswordInput.value = "";
    confirmPasswordInput.focus();
  }

  function closeModal() {
    modal.hidden = true;
  }

  triggerBtn.addEventListener("click", () => {
    if (runningJobId) {
      stopSimulation();
    } else {
      openModal();
    }
  });

  confirmCancel.addEventListener("click", closeModal);
  modal.addEventListener("click", (event) => {
    if (event.target === modal) closeModal();
  });

  confirmSubmit.addEventListener("click", async () => {
    const minCount = parseInt(minCountInput.value, 10) || 1000;
    const maxCount = parseInt(maxCountInput.value, 10) || 5000;
    const password = confirmPasswordInput.value;
    closeModal();
    await startSimulation(minCount, maxCount, password);
  });

  async function startSimulation(minCount, maxCount, password) {
    triggerBtn.disabled = true;
    statSent.textContent = "0";
    statSuccess.textContent = "0";
    statFail.textContent = "0";
    setStatus("Iniciando simulação...", "live");
    drawLineChart(new Array(15).fill(0));

    const body = new URLSearchParams();
    body.set("min_count", minCount);
    body.set("max_count", maxCount);
    body.set("confirm_password", password);

    let response;
    try {
      response = await fetch("/admin/simulate", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body,
      });
    } catch (err) {
      setStatus("Falha de rede ao iniciar a simulação.", "danger");
      triggerBtn.disabled = false;
      return;
    }

    if (!response.ok) {
      const detail = await response.json().catch(() => ({}));
      setStatus(detail.detail || "Não foi possível iniciar a simulação.", "danger");
      triggerBtn.disabled = false;
      return;
    }

    const { job_id } = await response.json();
    runningJobId = job_id;
    finishingJobId = null;
    triggerBtn.textContent = "Parar simulação";
    triggerBtn.disabled = false;
    minCountInput.disabled = true;
    maxCountInput.disabled = true;

    eventSource = new EventSource(`/admin/simulate/stream/${job_id}`);
    eventSource.onmessage = (event) => {
      let data;
      try {
        data = JSON.parse(event.data);
      } catch (err) {
        setStatus("Atualização inválida recebida da simulação.", "danger");
        return;
      }
      updateProgress(data);
      if (data.done) {
        finishSimulation(data, job_id);
      }
    };
    eventSource.onerror = () => {
      if (runningJobId !== job_id || finishingJobId === job_id) return;
      finishSimulation(null, job_id);
      setStatus("Conexão de acompanhamento perdida.", "danger");
    };
  }

  async function stopSimulation() {
    if (!runningJobId) return;
    triggerBtn.disabled = true;
    setStatus("Encerrando simulação...", "live");
    try {
      const response = await fetch(`/admin/simulate/stop/${runningJobId}`, { method: "POST" });
      if (!response.ok) {
        const detail = await response.json().catch(() => ({}));
        setStatus(detail.detail || "Não foi possível parar a simulação.", "danger");
        triggerBtn.disabled = false;
      }
    } catch (err) {
      setStatus("Falha de rede ao parar a simulação.", "danger");
      triggerBtn.disabled = false;
    }
  }

  function finishSimulation(finalData, jobId = runningJobId) {
    if (finishingJobId === jobId) return;
    finishingJobId = jobId;
    if (eventSource) {
      eventSource.close();
      eventSource = null;
    }
    runningJobId = null;
    triggerBtn.textContent = "Iniciar simulação";
    triggerBtn.disabled = false;
    minCountInput.disabled = false;
    maxCountInput.disabled = false;
    if (finalData) {
      setStatus(
        `Parado — ${finalData.success} sucesso, ${finalData.fail} falhas, ${finalData.elapsed}s no total.`,
        null
      );
    } else if (runningJobId === null) {
      setStatus("Simulação encerrada.", null);
    }
  }

  function updateProgress(data) {
    if (!Array.isArray(data.buckets) || data.buckets.length < 2) return;
    statSent.textContent = data.sent;
    statSuccess.textContent = data.success;
    statFail.textContent = data.fail;
    drawLineChart(data.buckets);

    if (data.done) return;

    if (data.phase === "bursting") {
      setStatus(`Rajada em andamento — ${data.burst_size} requisições`, "live");
    } else {
      setStatus(`Aguardando próxima rajada em ${data.next_in}s`, "live");
    }
  }
})();
