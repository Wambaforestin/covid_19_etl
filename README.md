# MSPR : Développement et déploiement d'une application dans le respect du cahier des charges Client

1. Création d'un backEnd métier permettant le nettoyage et la visualisation des données.
2. Mise en place d'un processus ETL (covid-19)

![image](image-architecture/arch_etl.png)

![image](image-architecture/data_flow_etl.png)

# Schéma du CI/CD et test automatisé

```mermaid
graph TD
    A[Développeur push sur GitHub] --> B[Déclenchement GitHub Actions]
    B --> C[Exécution des tests unitaires (pytest)]
    C --> D{Résultat des tests}
    D -- Succès --> E[Déploiement validé]
    D -- Échec --> F[Build interrompu]

    E --> G[Notification équipe : ✅ Succès]
    F --> H[Notification équipe : ❌ Échec]

    style A fill:#f9f,stroke:#333,stroke-width:1px
    style B fill:#bbf,stroke:#333,stroke-width:1px
    style C fill:#bbf,stroke:#333,stroke-width:1px
    style D fill:#ffd,stroke:#333,stroke-width:1px
    style E fill:#cfc,stroke:#333,stroke-width:1px
    style F fill:#fbb,stroke:#333,stroke-width:1px
    style G fill:#cfc,stroke:#333,stroke-width:1px
    style H fill:#fbb,stroke:#333,stroke-width:1px
```	
