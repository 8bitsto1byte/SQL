-- Data Cleaning
-- Nettoyage de données
-- Limpieza de datos
USE world_layoffs;
SELECT * 
FROM layoffs
;

-- 1. Remove duplicates   2. Standarize the data    3. Look up for Null Values or blank values   4. Remove any irrelevant columns or rows
-- 1. Supprimer les doublons 2. Normaliser les données 3. Rechercher les valeurs nulles ou les valeurs vides 4. Supprimer les colonnes ou lignes inutiles
-- 1. Eliminar duplicados 2. Estandarizar los datos 3. Buscar valores nulos o en blanco 4. Eliminar columnas o líneas irrelevantes

-- Creating a staging table based on the layoffs table
-- Création d'une table de transition basée sur la table des layouts (licenciements)
-- Creación de una tabla de pruebas basada en la tabla de layouts (despidos)

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- Removing duplicates; Adding a row number to identify any duplicates by partitioning on 4 columns
-- Suppression des doublons ; Ajout d'un numéro de ligne pour identifier les doublons en cloisonnant sur 4 colonnes.
-- Eliminación de duplicados; adición de un número de fila para identificar los duplicados partiendo de 4 columnas

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- Creating a CTE to see which rows are duplicate
-- Création d'un CTE pour voir quelles lignes sont dupliquées
-- Creación de un CTE para ver qué filas están duplicadas

WITH duplciate_cte AS (
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplciate_cte
WHERE row_num > 1
;


-- Validate that those are in fact duplicates
-- Valider qu'il s'agit bien de doublons
-- Validar que esos son realmente duplicados

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

-- Creating another staging table to delete the duplicates
-- Création d'une autre table de transition pour supprimer les doublons
-- Crear otra tabla de preparación para eliminar los duplicados

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2;
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging; 


SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Deleting all the duplicates (where row_num > 1)
-- Supprimer tous les doublons (où row_num > 1)
-- Eliminación de todos los duplicados (donde row_num > 1)

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Confirming
-- Confirmation
-- Confirmación
SELECT *
FROM layoffs_staging2;

-- Standarizing data
-- Normalisation des données
-- Normalización de datos

SELECT DISTINCT company
FROM layoffs_staging2;

-- Deleting spaces before the characters
-- Supprimer les espaces avant les caractères
-- Suprimir los espacios antes de los caracteres

SELECT DISTINCT (TRIM(company))
FROM layoffs_staging2;

SELECT company, TRIM(company)
FROM layoffs_staging2;


-- Updating the column with the new column information
-- Mise à jour de la colonne avec les informations de la nouvelle colonne
-- Actualización de la columna con la información de la nueva columna

UPDATE layoffs_staging2
SET company = TRIM(company);

-- Verifying the industry column
-- Vérification de la colonne « industry »
-- Verificación de la columna "industry"

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- We saw that 'crypto' could also be 'crypto currency' or 'cryptocurrency'.
-- Nous avons vu que « crypto » pouvait aussi être « crypto currency » ou « cryptocurrency ».
-- Vimos que "crypto" también podía ser "crypto currency" o "cryptocurrency".

SELECT *
FROM layoffs_staging2
WHERE industry like 'crypto%'
;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%'
;
-- Verification
-- Verification
-- Verificación

SELECT distinct industry
FROM layoffs_staging2;

SELECT distinct location
FROM layoffs_staging2
ORDER BY location;

SELECT distinct country
FROM layoffs_staging2
ORDER BY 1;

-- Country 'United States' and 'United States.'

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2;

-- Formatting the date as DATE
-- Formater la date en DATE
-- Formatear la fecha como DATE

SELECT
	`date`,
	str_to_date(`date`,'%m/%d/%Y') AS formatted_date
FROM 
layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`,'%m/%d/%Y');
-- Validating
-- Valider
-- Validar

SELECT
	`date`
FROM layoffs_staging2;

-- Altering the data type in the column
-- Modification du type de données dans la colonne
-- Modificación del tipo de datos de la columna

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Populating data with the data on the same table comparing the company and the location to guess the industry
-- Remplir les données avec les données du même tableau en comparant l'entreprise et la localisation pour deviner son industrie
-- Rellenar los datos con los datos de la misma tabla comparando la empresa y la ubicación para adivinar la industria

SELECT *
FROM layoffs_staging2 as t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '') 
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 as t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '') 
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airb%';

-- Deleting all the rows that we do not need for the purpose of this project
-- Suppression de toutes les lignes dont nous n'avons pas besoin dans le cadre de ce projet
-- Borrar todas las filas que no necesitamos para este proyecto.

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL;
    
-- Dropping a column from the dataset (because we don't need it anymore)
-- Supprimer une colonne de l'ensemble de données (parce que nous n'en avons plus besoin)
-- Eliminar una columna del conjunto de datos (porque ya no la necesitamos)

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;
