export async function exportReportToPDF() {
  const el = document.querySelector("article")
  if (!el) return

  const html2canvas = (await import("html2canvas")).default
  const { jsPDF } = await import("jspdf")

  const canvas = await html2canvas(el as HTMLElement, {
    backgroundColor: "#0f172a",
    scale: 2,
    useCORS: true,
  })

  const imgData = canvas.toDataURL("image/png")
  const imgWidth = 210 // A4 width in mm
  const pageHeight = 297 // A4 height in mm
  const imgHeight = (canvas.height * imgWidth) / canvas.width

  const pdf = new jsPDF("p", "mm", "a4")
  let heightLeft = imgHeight
  let position = 0

  pdf.addImage(imgData, "PNG", 0, position, imgWidth, imgHeight)
  heightLeft -= pageHeight

  while (heightLeft > 0) {
    position = heightLeft - imgHeight
    pdf.addPage()
    pdf.addImage(imgData, "PNG", 0, position, imgWidth, imgHeight)
    heightLeft -= pageHeight
  }

  pdf.save("comp-insight-report.pdf")
}
