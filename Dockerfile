FROM public.ecr.aws/zinclabs/zincsearch:latest

VOLUME ["/data"]

ENV ZINC_FIRST_ADMIN_USER=admin
ENV ZINC_FIRST_ADMIN_PASSWORD=admin

EXPOSE 4080

CMD ["zincsearch"]