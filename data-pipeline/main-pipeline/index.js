let stockData = []; // Data from the original endpoint
let anomalyData = []; // Data from the new endpoint (abnormal/normal and prices)
let currentPage = 0;
let currentPageLine = 0;
const companiesPerPage = 10;
let anomalyClusterChart = null;
let lineChart = null; // Initialize lineChart as null
let barChart = null; // Initialize barChart as null
let lineChartAll = null; // Initialize lineChartAll as null
let heatmapChart = null; // Declare this globally outside the function
let priceMAChart = null;
// Fetch data from the original endpoint
async function fetchStockData() {
  try {
    const response = await fetch("http://localhost:5000/api/cse_data"); // Original endpoint
    const data = await response.json();
    stockData = Object.values(data).flat(); // Flatten the data into an array of records
    populateDropdown(stockData);
  } catch (error) {
    console.error("Error fetching stock data:", error);
    alert("Failed to fetch stock data.");
  }
}

// Fetch data from the new endpoint
async function fetchAnomalyData() {
  try {
    const response = await fetch("http://localhost:5000/api/analyzed_data"); // New endpoint
    const data = await response.json();
    anomalyData = data; // Store the data for the bar chart and line chart
    renderBarChart();
    renderLineChartAll();
    renderHeatmap();
    renderPriceMAChart();
    renderAnomalyClusters();
  } catch (error) {
    console.error("Error fetching anomaly data:", error);
    console.log("Failed to fetch anomaly data.");
  }
}
const updateStats = () => {};
let highLowChart = null;
// let companySelectHighLow = document.getElementById("companySelectHighLow");

// Modify the populateDropdown function to populate both selects
function populateDropdown(data) {
  const companySet = new Set();
  data.forEach((record) => companySet.add(record.Company_Name));

  // Populate both dropdowns
  [companySelect].forEach((select) => {
    select.innerHTML = '<option value="">Select a Company</option>';
    companySet.forEach((company) => {
      const option = document.createElement("option");
      option.value = company;
      option.textContent = company;
      select.appendChild(option);
    });
  });

  // Add event listener for the new dropdown
  // companySelectHighLow.addEventListener("change", () => {
  //   const selectedCompany = companySelectHighLow.value;
  //   if (selectedCompany) {
  //     updateHighLowChart(selectedCompany);
  //     renderPriceMAChart(selectedCompany);
  //   }
  // });

  // Populate dropdown with unique company names
  companySet.forEach((company) => {
    const option = document.createElement("option");
    option.value = company;
    option.textContent = company;
    companySelect.appendChild(option);
  });
  companySelect.addEventListener("change", () => {
    const selectedCompany = companySelect.value;
    if (selectedCompany) {
      updateLineChart(selectedCompany);
    }
  });
}

function renderStats(data) {
  document.getElementById("companyName").textContent = data.Company_Name || "-";
  document.getElementById("companySymbol").textContent = data.Symbol || "-";
  document.getElementById("highValue").textContent = data["High_(Rs_)"] || "-";
  document.getElementById("lowValue").textContent = data["Low_(Rs_)"] || "-";
  document.getElementById("priceChange").textContent =
    data["Change(Rs)"] || "-";
  document.getElementById("percentChange").textContent =
    data["Change_(%)"] || "-";
  document.getElementById("anomalyStatus").textContent = data.anomaly || "-";
  document.getElementById("zScore").textContent =
    data.z_score?.toFixed(2) || "-";
  document.getElementById("movingAvg").textContent =
    data.moving_avg?.toFixed(2) || "-";
}

/////////////////////////////////graph no 5

// Add this new function to handle the High/Low chart
function updateHighLowChart(companyName) {
  const companyData = stockData.filter(
    (record) => record.Company_Name === companyName
  );
  const ctx = document.getElementById("highLowChart").getContext("2d");

  // Sort data by date
  companyData.sort((a, b) => new Date(a.Date) - new Date(b.Date));

  const labels = companyData.map((record) => record.Date);
  const prices = companyData.map((record) => record["**Last_Trade_(Rs_)"]);
  const highs = companyData.map((record) => record["High_(Rs_)"]);
  const lows = companyData.map((record) => record["Low_(Rs_)"]);

  if (highLowChart) highLowChart.destroy();

  highLowChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: labels,
      datasets: [
        {
          label: `Price - ${companyName}`,
          data: prices,
          borderColor: "rgba(0, 0, 255, 1)",
          backgroundColor: "rgba(0, 0, 255, 0.2)",
          fill: false,
          tension: 0.1,
        },
        {
          label: `High - ${companyName}`,
          data: highs,
          borderColor: "rgba(75, 192, 192, 1)",
          backgroundColor: "rgba(0, 0, 0, 0.2)",
          fill: false,
          tension: 0.1,
        },
        {
          label: `Low - ${companyName}`,
          data: lows,
          borderColor: "rgba(255, 99, 132, 1)",
          backgroundColor: "rgba(255, 0, 0, 0.2)",
          fill: false,
          tension: 0.1,
        },
      ],
    },
    options: {
      responsive: true,
      scales: {
        x: { title: { display: true, text: "Date" } },
        y: { title: { display: true, text: "Price (Rs.)" } },
      },
    },
  });
}

///////////////////////////////graph no5
function updateLineChart(companyName) {
  const companyData = stockData.filter(
    (record) => record.Company_Name === companyName
  );
  // Sort data by date (if it's not already sorted)

  const labels = companyData.map((record) => record.Date);
  const prices = companyData.map((record) => record["**Last_Trade_(Rs_)"]);
  const changes = companyData.map((record) => record["Change(Rs)"]);

  const ctx = document.getElementById("lineChart").getContext("2d");

  // Destroy the existing chart if it exists
  if (lineChart) {
    lineChart.destroy();
  }

  // Create a new chart
  lineChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: labels,
      datasets: [
        {
          label: `Price - ${companyName}`,
          data: prices,
          borderColor: "rgba(255, 99, 132, 1)",
          backgroundColor: "rgba(255, 99, 132, 0.2)",
          fill: false,
          tension: 0.1,
        },
        {
          label: `Price Change - ${companyName}`,
          data: changes,
          borderColor: "rgba(75, 192, 192, 1)",
          backgroundColor: "rgba(75, 192, 192, 0.2)",
          fill: false,
          tension: 0.1,
        },
      ],
    },
    options: {
      responsive: true,
      scales: {
        x: { title: { display: true, text: "Date" } },
        y: { title: { display: true, text: "Price (Rs.) / Change (Rs.)" } },
      },
    },
  });

  // Update stats for the selected company
  const selectedCompany = companyData[companyData.length - 1];

  renderStats(selectedCompany);
}

function renderBarChart() {
  const ctx = document.getElementById("barChart").getContext("2d");
  const startIndex = currentPage * companiesPerPage;
  const endIndex = startIndex + companiesPerPage;
  const paginatedData = anomalyData.slice(startIndex, endIndex);

  const labels = paginatedData.map((record) => record.Company_Name);
  const prices = paginatedData.map((record) => record.price);

  // Destroy the existing chart if it exists
  if (barChart) {
    barChart.destroy();
  }

  // Create a new chart
  barChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels: labels,
      datasets: [
        {
          label: "Stock Price (Rs.)",
          data: prices,
          backgroundColor: "rgba(75, 192, 192, 0.2)",
          borderColor: "rgba(75, 192, 192, 1)",
          borderWidth: 1,
        },
      ],
    },
    options: {
      responsive: true,
      scales: {
        x: { title: { display: true, text: "Companies" } },
        y: { title: { display: true, text: "Price (Rs.)" } },
      },
    },
  });

  // Add click event to bars to show stats
  ctx.canvas.onclick = (event) => {
    const activePoint = barChart.getElementsAtEventForMode(
      event,
      "nearest",
      { intersect: true },
      false
    );
    if (activePoint.length > 0) {
      const index = activePoint[0].index;
      const selectedCompany = paginatedData[index];
      updateStats(selectedCompany);
    }
  };
}

function renderLineChartAll() {
  const ctx = document.getElementById("lineChartAll").getContext("2d");
  const startIndex = currentPageLine * companiesPerPage;
  const endIndex = startIndex + companiesPerPage;
  const paginatedData = anomalyData.slice(startIndex, endIndex);

  const labels = paginatedData.map((record) => record.Company_Name);
  const todayPrices = paginatedData.map((record) => record.price);
  const yesterdayPrices = paginatedData.map(
    (record) => record["Previous_Close_(Rs_)"]
  );

  // Destroy the existing chart if it exists
  if (lineChartAll) {
    lineChartAll.destroy();
  }

  // // Create a new chart
  lineChartAll = new Chart(ctx, {
    type: "line",
    data: {
      labels: labels,
      datasets: [
        {
          label: "Today's Price",
          data: todayPrices,
          borderColor: "rgba(75, 192, 192, 1)",
          backgroundColor: "rgba(75, 192, 192, 0.2)",
          fill: false,
          tension: 0.1,
        },
        {
          label: "Yesterday's Price",
          data: yesterdayPrices,
          borderColor: "rgba(255, 159, 64, 1)",
          backgroundColor: "rgba(255, 159, 64, 0.2)",
          fill: false,
          tension: 0.1,
        },
      ],
    },
    options: {
      responsive: true,
      scales: {
        x: { title: { display: true, text: "Companies" } },
        y: { title: { display: true, text: "Price (Rs.)" } },
      },
    },
  });
}
// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>HANDLE BUTTON EVENTS>>>>>>>>>>>>>>>>
// const handleClick = () => {console.log('>>>>> MUDALI NEW >>>')}
function handleNextButtonLineClick() {
  console.log(">>>>>>> handleNextButtonLineClick");
}
function handleNextButtonClick() {
  console.log(">>>>>>>> handleNextButtonClick >>>>");
}

// Add this code to replace the existing button event listeners

// Handle pagination for the bar chart (Today's Prices)
document.getElementById("prevButton").addEventListener("click", () => {
  if (currentPage > 0) {
    currentPage--;
    renderBarChart();
  }
});

document.getElementById("nextButton").addEventListener("click", () => {
  const maxPage = Math.ceil(anomalyData.length / companiesPerPage) - 1;
  if (currentPage < maxPage) {
    currentPage++;
    renderBarChart();
  }
});

// Handle pagination for the line chart (Today vs Yesterday)
document.getElementById("prevButtonLine").addEventListener("click", () => {
  if (currentPageLine > 0) {
    currentPageLine--;
    renderLineChartAll();
  }
});

document.getElementById("nextButtonLine").addEventListener("click", () => {
  const maxPageLine = Math.ceil(anomalyData.length / companiesPerPage) - 1;
  if (currentPageLine < maxPageLine) {
    currentPageLine++;
    renderLineChartAll();
  }
});

document
  .getElementById("nextButtonLine")
  .addEventListener("click", handleNextButtonLineClick);
document
  .getElementById("nextButton")
  .addEventListener("click", handleNextButtonClick);
///////////////////////////////////////////////////////////////////
// function renderPriceMAChart(companyName) {
//   const container = document.getElementById('priceMAChart');
//   const ctx = safeChartCleanup(priceMAChart, 'priceMAChart');

//   // Get fresh data
//   const companyData = anomalyData.filter(r => r.Company_Name === companyName);
// }
// Price vs MA Chart
function renderPriceMAChart(companyName) {
  const companyData = anomalyData.filter((r) => r.Company_Name === companyName);
  const ctx = document.getElementById("priceMAChart").getContext("2d");

  const labels = companyData.map((r) => r.Date);
  const prices = companyData.map((r) => r["**Last_Trade_(Rs_)"]);
  const ma5 = companyData.map((r) => r.moving_avg);

  if (priceMAChart) priceMAChart.destroy();

  priceMAChart = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Price",
          data: prices,
          borderColor: "rgba(86, 213, 177, 0.43)",
        },
        {
          label: "5-Day MA",
          data: ma5,
          borderColor: "#e74c3c",
        },
      ],
    },
  });
}

// Ensure it's globally accessible

function renderHeatmap() {
  const container = document.getElementById("heatmapContainer"); // Use a container
  container.innerHTML = ""; // Clear previous content

  // Create new canvas
  const newCanvas = document.createElement("canvas");
  newCanvas.id = "heatmapChart";
  container.appendChild(newCanvas);
  const newCtx = newCanvas.getContext("2d");

  // Destroy existing chart if it exists
  if (heatmapChart instanceof Chart) {
    try {
      heatmapChart.destroy();
    } catch (e) {
      console.warn("Chart destruction error:", e);
    }
  }

  // Ensure anomalyData exists
  if (!anomalyData || anomalyData.length === 0) {
    console.warn("No data available for heatmap.");
    return;
  }
  console.log("data for heatmap", anomalyData);
  // Prepare data
  const data = anomalyData.map((d) => ({
    x: d.Company_Name,
    y: d["Change_(%)"] || 0,
    r: d.volume ? Math.sqrt(d.volume) / 10 : 5,
  }));

  // Create Chart
  heatmapChart = new Chart(newCtx, {
    type: "bubble",
    data: {
      datasets: [
        {
          label: "Companies",
          data,
          backgroundColor: (context) => {
            return context.raw.y >= 0
              ? "rgba(152, 254, 225, 0.43)"
              : "rgba(252, 126, 112, 0.5)";
          },
        },
      ],
    },
    options: {
      scales: {
        x: {
          type: "category",
          title: { display: true, text: "Companies" },
        },
        y: {
          title: { display: true, text: "% Change" },
        },
      },
    },
  });
}

// Call renderHeatmap when data is available
document.addEventListener("DOMContentLoaded", () => {
  if (anomalyData && anomalyData.length > 0) {
    renderHeatmap();
    console.log("data for heatmap", anomalyData);
  } else {
    console.warn("Waiting for data......");
  }
});

document.addEventListener("DOMContentLoaded", () => {
  if (anomalyData && anomalyData.length > 0) {
    renderPriceMAChart();
    console.log("data for PriceMAChart", anomalyData);
  } else {
    console.warn("Waiting for data. PriceMAChart.....");
  }
});

document.addEventListener("DOMContentLoaded", () => {
  renderHeatmap(); // Ensure it runs after the page loads
});

//////////////////////////////////////////////anomaly cluster chart

function renderAnomalyClusters() {
  const ctx = document.getElementById("anomalyClusterChart").getContext("2d");

  // Destroy existing chart
  if (anomalyClusterChart instanceof Chart) {
    anomalyClusterChart.destroy();
  }

  // Process data for clustering
  // const clusterData = anomalyData.reduce((acc, entry) => {
  //   if (entry.anomaly === "Anomaly") {
  //     const date = new Date(entry.timestamp.split("T")[0]); // Extract date
  //     const dateString = date.toISOString().split("T")[0];
  //     console.log(">>> DATEn STR >>", dateString);
  //     acc[dateString] = (acc[dateString] || 0) + 1;
  //   }
  //   return acc;
  // }, {});
  const clusterData = anomalyData.reduce((acc, entry) => {
    const timestamp = entry.timestamp;
    console.log(">>>>> Original Timestamp:", timestamp); // Log original timestamp

    // Skip invalid timestamps
    if (!timestamp || timestamp === "date" || timestamp === "dates") {
      console.log(">>>>>> Skipping invalid timestamp:", timestamp);
      return acc;
    }

    // Clean timestamp to make it a valid ISO date string by removing the part after the underscore
    const cleanedTimestamp = timestamp.split("_")[0]; // This removes everything after the '_'
    const date = new Date(cleanedTimestamp);
    console.log(">>>>> Parsed Date:", date); // Log parsed date

    if (
      entry.anomaly.trim().toLowerCase() === "anomaly" &&
      !isNaN(date.getTime())
    ) {
      const dateString = date.toISOString().split("T")[0];
      console.log(">>>>> Date String:", dateString); // Log the formatted date string

      acc[dateString] = (acc[dateString] || 0) + 1;
    }
    return acc;
  }, {});

  console.log("Cluster Data:", clusterData); // Log final clusterData

  console.log(">>>>> Cluster Data:", clusterData); // Log final clusterData

  // Convert to chart format
  const labels = Object.keys(clusterData).sort();
  const data = labels.map((date) => clusterData[date]);

  // Create chart
  anomalyClusterChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: labels,
      datasets: [
        {
          label: "Anomaly Clusters Over Time",
          data: data,
          borderColor: "#e74c3c",
          backgroundColor: "rgba(231, 76, 60, 0.2)",
          fill: true,
          tension: 0.4,
        },
      ],
    },
    options: {
      responsive: true,
      scales: {
        x: {
          title: {
            display: true,
            text: "Date",
          },
          type: "time",
          time: {
            unit: "day",
          },
        },
        y: {
          title: {
            display: true,
            text: "Number of Anomalies",
          },
          beginAtZero: true,
        },
      },
      plugins: {
        tooltip: {
          callbacks: {
            title: (context) => {
              return `Date: ${context[0].label}`;
            },
            label: (context) => {
              return `Anomalies: ${context.raw}`;
            },
          },
        },
      },
    },
  });
}

document.addEventListener("DOMContentLoaded", () => {
  if (anomalyData && anomalyData.length > 0) {
    renderAnomalyClusters();
    console.log("data for vlusters", anomalyData);
  } else {
    console.warn("Waiting for data. clusters.....");
  }
});

/////////////////////////////////////////////////////////////////////////////////

// Universal chart cleanup helper
function safeChartCleanup(chartInstance, canvasId) {
  if (chartInstance instanceof Chart) {
    try {
      chartInstance.destroy();
    } catch (e) {
      console.warn(`Error destroying ${canvasId} chart:`, e);
    }
  }

  const container = document.getElementById(canvasId);
  if (container) {
    // Remove existing canvas
    while (container.firstChild) {
      container.removeChild(container.firstChild);
    }
    // Create fresh canvas
    const newCanvas = document.createElement("canvas");
    newCanvas.id = canvasId;
    container.appendChild(newCanvas);
  }

  return newCanvas ? newCanvas.getContext("2d") : null;
}

// Updated render functions

// MA Crossover Signals
function renderCrossoverSignals() {
  const signals = anomalyData.map((d) => ({
    company: d.Company_Name,
    signal: d.ma_5 > d.ma_20 ? "Buy" : "Sell",
  }));

  const ctx = document.getElementById("crossoverChart").getContext("2d");

  if (crossoverChart) crossoverChart.destroy();

  crossoverChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels: signals.map((s) => s.company),
      datasets: [
        {
          label: "Trading Signal",
          data: signals.map((s) => (s.signal === "Buy" ? 1 : -1)),
          backgroundColor: (ctx) => (ctx.raw === 1 ? "#2ecc71" : "#e74c3c"),
        },
      ],
    },
  });
}

// Update initialization
// async function fetchAnomalyData2() {
//   try {
//     const response = await fetch("http://localhost:5000/api/analyzed_data");
//     const data = await response.json();
//     anomalyData = data;
//     renderBarChart();
//     renderLineChartAll();
//     renderHeatmap();
//     renderCrossoverSignals();
//     renderAnomalyClusters();
//     renderCorrelationMatrix();
//   } catch (error) {
//     console.error("Error fetching anomaly data:", error);
//   }
// }

// Update company change handler
companySelect.addEventListener("change", () => {
  const selectedCompany = companySelect.value;
  if (selectedCompany) {
    updateLineChart(selectedCompany);
    updateHighLowChart(selectedCompany);
    renderPriceMAChart(selectedCompany);
    renderAnomalyClusters(selectedCompany);
  }
});

// Fetch data on page load
fetchStockData();
fetchAnomalyData();
renderHeatmap();
