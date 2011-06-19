#!/bin/sh


/usr/bin/python ParseLog.py -l $1 > parse.txt
if [ $? -ne 0 ]
then
   exit
fi

cp report-template.tex report.tex
echo "\\begin{center} \\textbf{Percentage = $3\\%} \\end{center}" >> report.tex
cat parse.txt >> report.tex
#echo "\end{verbatim} \end{document}" >> report.tex

echo "\end{document}" >> report.tex
pdflatex report.tex
pdflatex report.tex


if [ $# -gt "1" ]
then
   echo "Report for $1" | mutt -a report.pdf -a SubHist.png -a sites.png -s "Report" $2
fi

