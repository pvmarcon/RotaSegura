(() => {
  const qrGeneratorForm = document.getElementById("qr-generator-form");
  const qrStudentId = document.getElementById("qr-student-id");
  const qrBusId = document.getElementById("qr-bus-id");
  const qrBoardingLocation = document.getElementById("qr-boarding-location");
  const qrValidity = document.getElementById("qr-validity");
  const qrPreview = document.getElementById("qr-code-preview");
  const qrOutputLabel = document.getElementById("qr-output-label");
  const qrToken = document.getElementById("qr-token");
  const qrCopyToken = document.getElementById("qr-copy-token");
  const qrGeneratorStatus = document.getElementById("qr-generator-status");
  const studentRegistrationForm = document.getElementById("student-registration-form");
  const studentRegistrationName = document.getElementById("student-registration-name");
  const studentRegistrationStatus = document.getElementById("student-registration-status");

  studentRegistrationForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const response = await fetch("/admin/alunos", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ student_name: studentRegistrationName.value.trim() }),
    });
    const data = await response.json().catch(() => ({}));
    studentRegistrationStatus.textContent = response.ok
      ? `${data.student_name} (ID: ${data.student_id}) cadastrado.`
      : data.detail || "Não foi possível cadastrar o aluno.";
    studentRegistrationStatus.className = `qr-generator-status ${response.ok ? "success" : "error"}`;
    if (response.ok) {
      if (!qrStudentId) {
        window.location.reload();
        return;
      }
      studentRegistrationName.value = "";
      qrStudentId.value = data.student_id;
      const history = document.querySelector(".student-history-list");
      const empty = document.querySelector(".student-history-empty");
      if (empty) empty.remove();
      let historyList = history;
      if (!historyList) {
        historyList = document.createElement("div");
        historyList.className = "student-history-list";
        document.querySelector(".student-history").appendChild(historyList);
      }
      const row = document.createElement("div");
      row.className = "student-history-row student-history-row--new";
      const name = document.createElement("strong");
      name.textContent = data.student_name;
      const id = document.createElement("span");
      id.textContent = `(ID: ${data.student_id})`;
      const created = document.createElement("small");
      created.textContent = "agora";
      row.append(name, id, created);
      historyList.prepend(row);
      const count = document.getElementById("student-history-count");
      if (count) count.textContent = `${historyList.children.length} registros`;
    }
  });

  const blockedStudentForm = document.getElementById("blocked-student-form");
  const blockedStudentInput = document.getElementById("blocked-student-id");
  const blockedStudentsList = document.getElementById("blocked-students-list");

  function updateBoardingStatuses(studentId, blocked) {
    document.querySelectorAll(`.status-tag[data-student-id="${CSS.escape(studentId)}"]`).forEach((status) => {
      status.textContent = blocked ? "Bloqueado" : "Confirmado";
      status.classList.toggle("status-tag--blocked", blocked);
    });
  }

  async function loadBlockedStudents() {
    if (!blockedStudentsList) return;
    const response = await fetch("/admin/alunos-bloqueados");
    if (!response.ok) return;
    const students = await response.json();
    blockedStudentsList.replaceChildren();
    if (!students.length) {
      blockedStudentsList.innerHTML = '<span class="table-empty">Nenhum aluno bloqueado.</span>';
      return;
    }
    students.forEach((studentId) => {
      const row = document.createElement("div");
      row.className = "blocked-student-row";
      const label = document.createElement("strong");
      label.textContent = studentId;
      const unblockButton = document.createElement("button");
      unblockButton.className = "table-action";
      unblockButton.type = "button";
      unblockButton.textContent = "Desbloquear";
      row.append(label, unblockButton);
      unblockButton.addEventListener("click", async () => {
        const result = await fetch(`/admin/alunos-bloqueados/${encodeURIComponent(studentId)}`, { method: "DELETE" });
        if (result.ok) {
          updateBoardingStatuses(studentId, false);
          loadBlockedStudents();
        }
      });
      blockedStudentsList.appendChild(row);
    });
  }

  blockedStudentForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const studentId = blockedStudentInput.value.trim();
    if (!studentId) return;
    const response = await fetch("/admin/alunos-bloqueados", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ student_id: studentId }),
    });
    if (response.ok) {
      blockedStudentInput.value = "";
      updateBoardingStatuses(studentId, true);
      loadBlockedStudents();
    }
  });
  loadBlockedStudents();

  const editModal = document.getElementById("boarding-edit-modal");
  const editStudentInput = document.getElementById("edit-student-id");
  const editBusInput = document.getElementById("edit-bus-id");
  const editBoardingAtInput = document.getElementById("edit-boarding-at");
  let editingBoardingId = null;

  function toDateTimeLocal(value) {
    const date = new Date(value);
    const pad = (number) => String(number).padStart(2, "0");
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
  }

  function closeEditModal() {
    editModal.hidden = true;
    editingBoardingId = null;
  }

  document.querySelectorAll(".edit-boarding").forEach((button) => {
    button.addEventListener("click", () => {
      editingBoardingId = button.dataset.id;
      editStudentInput.value = button.dataset.student;
      editBusInput.value = button.dataset.bus;
      editBoardingAtInput.value = toDateTimeLocal(button.dataset.created);
      editModal.hidden = false;
      editStudentInput.focus();
    });
  });

  document.querySelectorAll(".delete-boarding").forEach((button) => {
    button.addEventListener("click", async () => {
      button.disabled = true;
      const response = await fetch(`/admin/embarques/${button.dataset.id}`, { method: "DELETE" });
      if (response.ok) {
        button.closest("tr")?.remove();
      } else {
        button.disabled = false;
        alert("Não foi possível excluir o embarque.");
      }
    });
  });

  document.getElementById("edit-cancel")?.addEventListener("click", closeEditModal);
  editModal?.addEventListener("click", (event) => {
    if (event.target === editModal) closeEditModal();
  });
  document.getElementById("edit-submit")?.addEventListener("click", async () => {
    const studentId = editStudentInput.value.trim();
    const busId = editBusInput.value.trim();
    if (!editingBoardingId || !studentId || !busId || !editBoardingAtInput.value) return;
    const response = await fetch(`/admin/embarques/${editingBoardingId}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        student_id: studentId,
        bus_id: busId,
        boarding_at: new Date(editBoardingAtInput.value).toISOString(),
      }),
    });
    if (response.ok) {
      const row = document.querySelector(`.edit-boarding[data-id="${CSS.escape(editingBoardingId)}"]`)?.closest("tr");
      if (row) {
        row.querySelector(".boarding-student").textContent = `${studentId} (ID: ${studentId})`;
        row.querySelector(".boarding-student").dataset.studentId = studentId;
        row.querySelector(".status-tag").dataset.studentId = studentId;
        row.querySelector(".boarding-bus").textContent = busId;
        const editButton = row.querySelector(".edit-boarding");
        editButton.dataset.student = studentId;
        editButton.dataset.bus = busId;
        row.querySelector(".boarding-time").textContent = new Date(editBoardingAtInput.value).toLocaleDateString("pt-BR") + " " + new Date(editBoardingAtInput.value).toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" });
      }
      closeEditModal();
    } else alert("Não foi possível editar o embarque.");
  });

  if (qrGeneratorForm) {
    qrGeneratorForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const alunoId = qrStudentId.value.trim();
      const busId = qrBusId.value.trim();
      const boardingLocation = qrBoardingLocation.value.trim();
      const validadeSegundos = (parseInt(qrValidity.value, 10) || 5) * 60;
      qrGeneratorStatus.textContent = "Gerando...";
      qrGeneratorStatus.className = "qr-generator-status";

      let response;
      try {
        response = await fetch("/admin/gerar-qr", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ aluno_id: alunoId, bus_id: busId, boarding_location: boardingLocation, validade_segundos: validadeSegundos }),
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
      qrOutputLabel.textContent = `${data.aluno_nome} (ID: ${data.aluno_id}) · ${data.bus_id} · ${data.boarding_location} · válido por ${qrValidity.value} minuto(s)`;
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
  if (!canvas) return;
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
