# Job & Cluster Monitoring — deploy the DAB pipeline + publish Genie Code Skills.
#
#   make deploy   # bundle (jobs+dashboard) AND publish skills  -> one command
#   make refresh  # run the workload_monitoring_refresh job (populate tables + comments)
#
# Overridable: PROFILE, TARGET, SKILLS_ROOT
#   SKILLS_ROOT=/Workspace/.assistant/skills            # workspace-wide (needs admin, default)
#   SKILLS_ROOT=/Users/<you>/.assistant/skills          # personal
PROFILE     ?= DEFAULT
TARGET      ?= dev
SKILLS_ROOT ?= /Workspace/.assistant/skills

.PHONY: deploy bundle skills refresh help

help:
	@echo "make deploy   - DAB deploy (jobs+dashboard) + publish Genie Code skills"
	@echo "make bundle   - DAB deploy only (jobs + dashboard)"
	@echo "make skills   - publish skills/ to \$$(SKILLS_ROOT)"
	@echo "make refresh  - run the workload_monitoring_refresh job"
	@echo "vars: PROFILE=$(PROFILE) TARGET=$(TARGET) SKILLS_ROOT=$(SKILLS_ROOT)"

deploy: bundle skills   ## full lifecycle: pipeline + skills

bundle:                 ## jobs + dashboard via DAB
	databricks bundle deploy --profile $(PROFILE) --target $(TARGET)

skills:                 ## Genie Code skills -> .assistant/skills
	databricks workspace import-dir skills "$(SKILLS_ROOT)" --overwrite --profile $(PROFILE)

refresh:                ## re-run analysis + re-seed object comments
	databricks bundle run workload_monitoring_refresh --profile $(PROFILE) --target $(TARGET)
