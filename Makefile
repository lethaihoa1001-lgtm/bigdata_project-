.PHONY: data up down pdf

data:
	bash scripts/download_data.sh

up:
	docker compose up -d

down:
	docker compose down

# Cần pandoc và xelatex. Đổi mainfont nếu máy không có Times New Roman.
pdf:
	pandoc report/*.md -o baocao.pdf --toc --pdf-engine=xelatex -V mainfont="Times New Roman" -V geometry:margin=2.5cm
