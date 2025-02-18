class utils:
    """ "
    This class contains all common function for the pipeline data processing for several outputs like
    VIDEO_ID, TRAFFIC_SOURCE, DEVICE_TYPE, DEMOGRAPHICS and REGION.

    """

    def __init__(self, session):
        self.session = session

    def create_log_table(self) -> None:
        """Create a logging table."""
        self.session.sql(
            """
            CREATE OR REPLACE TABLE PROCESSING_LOG (
                SR_NO NUMBER AUTOINCREMENT,
                TIMESTAMP TIMESTAMP,
                MESSAGE STRING
            )
        """
        ).collect()

    def log_message(self, message: str) -> None:
        """Log a message into the temporary logging table."""
        log_sql = f"""
            INSERT INTO PROCESSING_LOG (TIMESTAMP, MESSAGE)
            VALUES (CURRENT_TIMESTAMP(), '{message.replace("'", "''")}')
        """
        self.session.sql(log_sql).collect()

    def map_video_title(self, query: str, metadata_table: str) -> str:
        """
        Function:
            * Maps VIDEO_TITLE using common VIDEO_ID
        Params:
            * query : Input Query
            * metadata_table : Metadata table path
        Returns:
            * Mapped Video_title query
        """
        return f"""
            SELECT 
                a.*,
                TO_TIMESTAMP(b.TIME_UPLOADED, 'YYYY/MM/DD HH24:MI:SS') AS PUBLISHED_DATE,
                b.VIDEO_TITLE,
                b.VIDEO_LENGTH AS DURATION
            FROM 
                ({query}) a
            LEFT JOIN {metadata_table} b
                ON a.VIDEO_ID = b.VIDEO_ID
        """

    def map_channel_names(self, query: str, metadata_table: str) -> str:
        """
        Function:
            * Maps CHANNEL_NAME using common CHANNEL_ID
        Params:
            * query : Input Query
            * metadata_table : Metadata table path
        Returns:
            * Mapped CHANNEL_NAME query
        """
        return f"""
            WITH ranked_metadata AS (
                SELECT 
                    b.CHANNEL_ID,
                    b.CHANNEL_DISPLAY_NAME,
                    ROW_NUMBER() OVER (PARTITION BY b.CHANNEL_ID ORDER BY b.TIME_UPLOADED DESC) AS rn
                FROM 
                    {metadata_table} b
            )
            SELECT 
                a.*,
                b.CHANNEL_DISPLAY_NAME AS CHANNEL_NAME
            FROM 
                ({query}) a
            LEFT JOIN ranked_metadata b
                ON a.CHANNEL_ID = b.CHANNEL_ID
                AND b.rn = 1
            """

    def extract_PPL_ID(self, YT_metadata: str, ppl_table: str) -> str:
        """
        Function:
            * This function extracts PPL_ID/PROGRAM_ID from CUSTOM_ID and THIRD_PARTY_ID
            * This function also gets DOMO_SERIES_NAME from case statement where SERIES_NAME from PPL table is not found.
        Params:
            * YT_metadata : Path of metadata table
            * ppl_table : Path of PPL Table.
        """
        return f"""
            SELECT
                a.VIDEO_ID,
                a.CUSTOM_ID,
                a.THIRD_PARTY_VIDEO_ID,
                a.VIDEO_TITLE,
                a.VIDEO_LENGTH AS DURATION,
                a.TIME_UPLOADED AS PUBLISHED_DATE,
                b.SERIES_ID,
                b.PARENT_SERIES_ID,
                b.SERIES_NAME,
                CASE
                    WHEN CUSTOM_ID LIKE '%_PGRMID_%' 
                        AND TRY_CAST(SPLIT_PART(CUSTOM_ID, '_', 3) AS INT) IS NOT NULL 
                        AND LEN(SPLIT_PART(CUSTOM_ID, '_', 3)) BETWEEN 4 AND 6 THEN 
                        SPLIT_PART(CUSTOM_ID, '_', 3)::INT

                    WHEN THIRD_PARTY_VIDEO_ID LIKE '%_%_%' 
                        AND TRY_CAST(SPLIT_PART(THIRD_PARTY_VIDEO_ID, '_', 1) AS INT) IS NOT NULL 
                        AND LEN(SPLIT_PART(THIRD_PARTY_VIDEO_ID, '_', 1)) BETWEEN 4 AND 6 THEN 
                        SPLIT_PART(THIRD_PARTY_VIDEO_ID, '_', 1)::INT
                        
                    WHEN TRY_CAST(THIRD_PARTY_VIDEO_ID AS NUMBER) IS NOT NULL 
                        AND TRY_CAST(THIRD_PARTY_VIDEO_ID AS NUMBER) <> 0 
                        AND LEN(TRY_CAST(THIRD_PARTY_VIDEO_ID AS NUMBER)) BETWEEN 4 AND 6 THEN 
                        TRY_CAST(THIRD_PARTY_VIDEO_ID AS NUMBER)

                    WHEN TRY_CAST(CUSTOM_ID AS NUMBER) IS NOT NULL AND TRY_CAST(THIRD_PARTY_VIDEO_ID AS NUMBER) IS NULL 
                        AND TRY_CAST(CUSTOM_ID AS NUMBER) <> 0 
                        AND LEN(TRY_CAST(CUSTOM_ID AS NUMBER)) BETWEEN 5 AND 6 THEN 
                        TRY_CAST(CUSTOM_ID AS NUMBER)
                
                    WHEN THIRD_PARTY_VIDEO_ID LIKE '%_%_%_%' 
                        AND TRY_CAST(SPLIT_PART(THIRD_PARTY_VIDEO_ID, '_', 2) AS INT) IS NOT NULL THEN 
                        SPLIT_PART(THIRD_PARTY_VIDEO_ID, '_', 2)::INT

                    WHEN CUSTOM_ID LIKE '%_%' 
                        AND TRY_CAST(SPLIT_PART(CUSTOM_ID, '_', 1) AS INT) IS NOT NULL 
                        AND LEN(SPLIT_PART(CUSTOM_ID, '_', 1)) BETWEEN 4 AND 6 THEN 
                        SPLIT_PART(CUSTOM_ID, '_', 1)::INT
                ELSE NULL
            END AS EXTRACTED_PPL_ID,
            CASE 
                when lower(VIDEO_TITLE) like '%60 days in%' then '60 Days In'
                when lower(VIDEO_TITLE) like '%#textmewhenyougethome%' or lower(VIDEO_TITLE) like '%textmewhenyougethome%' then '#TextMeWhenYouGetHome'
                when lower(VIDEO_TITLE) like '%10 things you didn''t know about%' then '10 Things You Didn''t Know About'
                when lower(VIDEO_TITLE) like '%12 men of christmas%' then '12 Men of Christmas'
                when lower(VIDEO_TITLE) like '%catwalk%' then '24-Hour Catwalk'
                when lower(VIDEO_TITLE) like '%5 guys a week%' then '5 Guys a Week'
                when lower(VIDEO_TITLE) like '%7 days of sex%' then '7 Days of Sex'
                when lower(VIDEO_TITLE) like '%a question of love%' then 'A Question of Love'
                when lower(VIDEO_TITLE) like '%a seven deadly sins story%' then 'A Seven Deadly Sins Story'
                when lower(VIDEO_TITLE) like '%abraham lincoln%' then 'Abraham Lincoln'
                when lower(VIDEO_TITLE) like '%adam eats%' then 'Adam Eats'
                when lower(VIDEO_TITLE) like '%adults adopting adults%' then 'Adults Adopting Adults'
                when lower(VIDEO_TITLE) like '%against the wall%' then 'Against the Wall'
                when lower(VIDEO_TITLE) like '%airline%' then 'Airline' 
                when lower(VIDEO_TITLE) like '%alaska off road warriors%' then 'Alaska Off Road Warriors'
                when lower(VIDEO_TITLE) like '%alaska pd%' then 'Alaska PD'
                when lower(VIDEO_TITLE) like '%aliencon%' then 'AlienCon'
                when lower(VIDEO_TITLE) like '%alone:%' then 'Alone'
                when lower(VIDEO_TITLE) like '%aly raisman%' then 'Aly Raisman'
                when lower(VIDEO_TITLE) like '%america 101%' then 'America 101'
                when lower(VIDEO_TITLE) like '%america unearthed%' then 'America Unearthed'
                when lower(VIDEO_TITLE) like '%america: promised land%' then 'America: Promised Land'
                when lower(VIDEO_TITLE) like '%america: the story of us%' or lower(VIDEO_TITLE) like '%america the story of us%' then 'America: The Story Of Us'
                when lower(VIDEO_TITLE) like '%book of secrets%' then 'Americas Book of Secrets'
                when lower(VIDEO_TITLE) like '%top dog%' then 'America’s Top Dog'
                when lower(VIDEO_TITLE) like '%american daredevils%' then 'American Daredevils'
                when lower(VIDEO_TITLE) like '%american freedom stories%' then 'American Freedom Stories'
                when lower(VIDEO_TITLE) like '%american haunting%' then 'American Haunting'
                when lower(VIDEO_TITLE) like '%american hoggers%' then 'American Hoggers'
                when lower(VIDEO_TITLE) like '%american justice%' or lower(VIDEO_TITLE) like '%dentist accused of hitting husband with her car%' then 'American Justice'
                when lower(VIDEO_TITLE) like '%american pickers%' or lower(VIDEO_TITLE) like '%america pickers%' then 'American Pickers'
                when lower(VIDEO_TITLE) like '%american restoration%' then 'American Restoration'
                when lower(VIDEO_TITLE) like '%american ripper%' then 'American Ripper'
                when lower(VIDEO_TITLE) like '%american takedown%' then 'American Takedown'
                when lower(VIDEO_TITLE) like '%american wiseass%' then 'American Wiseass'
                when lower(VIDEO_TITLE) like '%america''s war on drugs%' then 'America''s War on Drugs'
                when lower(VIDEO_TITLE) like '%amish grace%' then 'Amish Grace'
                when lower(VIDEO_TITLE) like '%an animal saved my life%' then 'An Animal Saved My Life'
                when lower(VIDEO_TITLE) like '%ancient aliens%' then 'Ancient Aliens'
                when lower(VIDEO_TITLE) like '%ancient discoveries%' then 'Ancient Discoveries'
                when lower(VIDEO_TITLE) like '%ancient impossible%' then 'Ancient Impossible'
                when lower(VIDEO_TITLE) like '%ancient mysteries%' then 'Ancient Mysteries'
                when lower(VIDEO_TITLE) like '%ancient recipes%' then 'Ancient Recipes'
                when lower(VIDEO_TITLE) like '%ancient top 10%' then 'Ancient Top 10'
                when lower(VIDEO_TITLE) like '%ancient workouts%' then 'Ancient Workouts'
                when lower(VIDEO_TITLE) like '%ancients behaving badly%' then 'Ancients Behaving Badly'
                when lower(VIDEO_TITLE) like '%ann rule''s "too late to say goodbye"%' then 'Ann Rule''s "Too Late to Say Goodbye"'
                when lower(VIDEO_TITLE) like '%appalachian outlaws%' then 'Appalachian Outlaws'
                when lower(VIDEO_TITLE) like '%army wives%' then 'Army Wives'
                when lower(VIDEO_TITLE) like '%arranged%' then 'Arranged'
                when lower(VIDEO_TITLE) like '%ask a celebrity%' then 'Ask A Celebrity'
                when lower(VIDEO_TITLE) like '%ask history%' then 'Ask History'
                when lower(VIDEO_TITLE) like '%assembly required%' then 'Assembly Required'
                when lower(VIDEO_TITLE) like '%atlanta plastic%' then 'Atlanta Plastic'
                when lower(VIDEO_TITLE) like '%audc%' or lower(VIDEO_TITLE) like '%ultimate dance competition%' then 'AUDC'
                when lower(VIDEO_TITLE) like '%austin and santino''s video blog%' then 'Austin and Santino''s Video Blog'
                when lower(VIDEO_TITLE) like '%ax men%' then 'Ax Men'
                when lower(VIDEO_TITLE) like '%bad ink%' then 'Bad Ink'
                when lower(VIDEO_TITLE) like '%bag of bones%' then 'Bag of Bones'
                when lower(VIDEO_TITLE) like '%bamazon%' then 'Bamazon'
                when lower(VIDEO_TITLE) like '%baps%' then 'BAPs'
                when lower(VIDEO_TITLE) like '%barbarians rising%' then 'Barbarians Rising'
                when lower(VIDEO_TITLE) like '%barry''d treasure%' then 'Barry''d Treasure'
                when lower(VIDEO_TITLE) like '%barter kings%' then 'Barter Kings'
                when lower(VIDEO_TITLE) like '%bates motel%' then 'Bates Motel'
                when lower(VIDEO_TITLE) like '%battle 360%' then 'Battle 360'
                when lower(VIDEO_TITLE) like '%battle of the christmas movie stars%' then 'Battle of the Christmas Movie Stars'
                when lower(VIDEO_TITLE) like '%battlefield detectives%' then 'Battlefield Detectives'
                when lower(VIDEO_TITLE) like '%battles bc%' or lower(VIDEO_TITLE) like '%battles b.c.%' then 'Battles BC'
                when lower(VIDEO_TITLE) like '%be the boss%' then 'Be the BOSS'
                when lower(VIDEO_TITLE) like '%rookie year%' then 'Behind Bars: Rookie Year'
                when lower(VIDEO_TITLE) like '%behind bars%' then 'Behind Bars'
                when lower(VIDEO_TITLE) like '%behind the seams%' then 'Behind the Seams'
                when lower(VIDEO_TITLE) like '%best in bridal%' then 'Best in Bridal'
                when lower(VIDEO_TITLE) like '%bet you didn''t know%' then 'Bet You Didn''t Know'
                when lower(VIDEO_TITLE) like '%betty white''s off their rockers%' then 'Betty White''s Off Their Rockers'
                when lower(VIDEO_TITLE) like '%beyond oak island%' then 'Beyond Oak Island'
                when lower(VIDEO_TITLE) like '%beyond scared straight%' then 'Beyond Scared Straight'
                when lower(VIDEO_TITLE) like '%beyond the headlines%' then 'Beyond the Headlines'
                when lower(VIDEO_TITLE) like '%bible secrets revealed%' then 'Bible Secrets Revealed'
                when lower(VIDEO_TITLE) like '%big easy motors%' then 'Big Easy Motors'
                when lower(VIDEO_TITLE) like '%big history%' then 'Big History'
                when lower(VIDEO_TITLE) like '%big rig bounty hunters%' then 'Big Rig Bounty Hunters'
                when lower(VIDEO_TITLE) like '%big shrimpin%' then 'Big Shrimpin'
                when lower(VIDEO_TITLE) like '%big smo%' then 'Big Smo'
                when lower(VIDEO_TITLE) like '%big women%' then 'Big Women'
                when lower(VIDEO_TITLE) like '%biggest battles of wwii%' then 'Biggest Battles of WWII'
                when lower(VIDEO_TITLE) like '%biggie%' then 'Biggie'
                when lower(VIDEO_TITLE) like '%biker battleground phoenix%' then 'Biker Battleground Phoenix'
                when lower(VIDEO_TITLE) like '%billion dollar wreck%' then 'Billion Dollar Wreck'
                when lower(VIDEO_TITLE) like '%billy the exterminator%' then 'Billy The Exterminator'
                when lower(VIDEO_TITLE) like '%black patriots%' then 'Black Patriots'
                when lower(VIDEO_TITLE) like '%#blacklove%' then 'BlackLove'
                when lower(VIDEO_TITLE) like '%blindspot%' then 'Blindspot'
                when lower(VIDEO_TITLE) like '%blood and glory%' then 'Blood and Glory'
                when lower(VIDEO_TITLE) like '%blood money%' then 'Blood Money'
                when lower(VIDEO_TITLE) like '%blush%' then 'Blush'
                when lower(VIDEO_TITLE) like '%every little step%'  or lower(VIDEO_TITLE) like '%bobby brown%' then 'Bobby Brown'
                when lower(VIDEO_TITLE) like '%bonnie & clyde%' then 'Bonnie & Clyde'
                when lower(VIDEO_TITLE) like '%booked: first day in%' then 'Booked' 
                when lower(VIDEO_TITLE) like '%bordertown%' then 'Bordertown'
                when lower(VIDEO_TITLE) like '%born behind bars%' and lower(VIDEO_TITLE) not like '%behind bars%' then 'Born Behind Bars'
                when lower(VIDEO_TITLE) like '%born in the wild%' then 'Born in the Wild'
                when lower(VIDEO_TITLE) like '%born this way%' then 'Born This Way'
                when lower(VIDEO_TITLE) like '%decoded%' then 'Brad Meltzer’s Decoded'
                when lower(VIDEO_TITLE) like '%brandi and jarrod%' then 'Brandi and Jarrod'
                when lower(VIDEO_TITLE) like '%breaking boston%' then 'Breaking Boston'
                when lower(VIDEO_TITLE) like '%breakup diaries%' then 'Breakup Diaries'
                when lower(VIDEO_TITLE) like '%bride & prejudice%' then 'Bride & Prejudice'
                when lower(VIDEO_TITLE) like '%bring it!%' or lower(VIDEO_TITLE) like '%bring it%' then 'Bring It!'
                when lower(VIDEO_TITLE) like '%bringing up ballers%' then 'Bringing Up Ballers'
                when lower(VIDEO_TITLE) like '%brothers in arms%' then 'Brothers in Arms'
                when lower(VIDEO_TITLE) like '%buried in barstow%' then 'Buried In Barstow'
                when lower(VIDEO_TITLE) like '%buried with love%' then 'Buried With Love'
                when lower(VIDEO_TITLE) like '%knights templar & the holy grail%' or lower(VIDEO_TITLE) like '%knights templar and the holy grail%' then 'Buried: Knights Templar and the Holy Grail'
                when lower(VIDEO_TITLE) like '%cajun justice%' then 'Cajun Justice'
                when lower(VIDEO_TITLE) like '%car hunters%' then 'Car Hunters'
                when lower(VIDEO_TITLE) like '%celebrity buzz%' then 'Celebrity Buzz'
                when lower(VIDEO_TITLE) like '%celebrity ghost stories%' and lower(VIDEO_TITLE) not like '%celebrity house hunting%' then 'Celebrity Ghost Stories'
                when lower(VIDEO_TITLE) like '%celebrity house hunting%' then 'Celebrity House Hunting'
                when lower(VIDEO_TITLE) like '%cellmate secrets%' then 'Cellmate Secrets'
                when lower(VIDEO_TITLE) like '%cement heads%' then 'Cement Heads'
                when lower(VIDEO_TITLE) like '%chasing mummies%' then 'Chasing Mummies'
                when lower(VIDEO_TITLE) like '%chasing nashville%' then 'Chasing Nashville'
                when lower(VIDEO_TITLE) like '%cheerleader generation%' then 'Cheerleader Generation'
                when lower(VIDEO_TITLE) like '%cheerleader nation%' then 'Cheerleader Nation'
                when lower(VIDEO_TITLE) like '%child genius%' then 'Child Genius'
                when lower(VIDEO_TITLE) like '%chloe does it%' then 'Chloe Does It'
                when lower(VIDEO_TITLE) like '%christmas through the decades%' then 'Christmas Through the Decades'
                when lower(VIDEO_TITLE) like '%cities of the underworld%' then 'Cities of the Underworld'
                when lower(VIDEO_TITLE) like '%city confidential%' then 'City Confidential'
                when lower(VIDEO_TITLE) like '%civil war combat%' then 'Civil War Combat'
                when lower(VIDEO_TITLE) like '%clash of the gods%' then 'Clash of the Gods'
                when lower(VIDEO_TITLE) like '%cold case files%' then 'Cold Case Files'
                when lower(VIDEO_TITLE) like '%colosseum%' then 'Colosseum'
                when lower(VIDEO_TITLE) like '%come dine with me%' then 'Come Dine With Me'
                when lower(VIDEO_TITLE) like '%coming home%' then 'Coming Home'
                when lower(VIDEO_TITLE) like '%command decisions%' then 'Command Decisions'
                when lower(VIDEO_TITLE) like '%cook yourself thin recipes%' then 'Cook Yourself Thin Recipes'
                when lower(VIDEO_TITLE) like '%coroner"s report%' then 'Coroner’s Report'
                when lower(VIDEO_TITLE) like '%history countdown%' then 'Countdown'
                when lower(VIDEO_TITLE) like '%countdown to armageddon%' then 'Countdown to Armageddon'
                when lower(VIDEO_TITLE) like '%countdown to the apocalypse%' then 'Countdown to the Apocalypse'
                when lower(VIDEO_TITLE) like '%counting cars%' then 'Counting Cars'
                when lower(VIDEO_TITLE) like '%country buck$%' then 'Country Buck"$"'
                when lower(VIDEO_TITLE) like '%court cam%' or lower(VIDEO_TITLE) like '%under oath%' then 'Court Cam: Under Oath'
                when lower(VIDEO_TITLE) like '%court cam%' and not lower(VIDEO_TITLE) like '%under oath%' then 'Court Cam'
                when lower(VIDEO_TITLE) like '%court night live%' then 'Court Night Live'
                when lower(VIDEO_TITLE) like '%cowboys & outlaws%' then 'Cowboys & Outlaws'
                when lower(VIDEO_TITLE) like '%crafted%' then 'Crafted'
                when lower(VIDEO_TITLE) like '%crazy hearts: nashville%' then 'Crazy Hearts: Nashville'
                when lower(VIDEO_TITLE) like '%crime 360%' then 'Crime 360'
                when lower(VIDEO_TITLE) like '%crime by the numbers%' then 'Crime by the Numbers'
                when lower(VIDEO_TITLE) like '%criss angel%' then 'Criss Angel Mindfreak'
                when lower(VIDEO_TITLE) like '%cryptid%' then 'Cryptid'
                when lower(VIDEO_TITLE) like '%cults and extreme belief%' then 'Cults and Extreme Belief'
                when lower(VIDEO_TITLE) like '%curse of oak island%' then 'Curse of Oak Island'
                when lower(VIDEO_TITLE) like '%cursed: the bell witch%' then 'Cursed: The Bell Witch'
                when lower(VIDEO_TITLE) like '%custer%' then 'Custer'
                when lower(VIDEO_TITLE) like '%customer wars%' then 'Customer Wars'
                when lower(VIDEO_TITLE) like '%dallas swat%' then 'Dallas SWAT'
                when lower(VIDEO_TITLE) like '%damien%' then 'Damien'
                when lower(VIDEO_TITLE) like '%dance moms%' or lower(VIDEO_TITLE) like '%aldc%' or lower(VIDEO_TITLE) like '%who is the lead?%' then 'Dance Moms'
                when lower(VIDEO_TITLE) like '%dangerous missions%' then 'Dangerous Missions'
                when lower(VIDEO_TITLE) like '%dare to wear%' then 'Dare to Wear'
                when lower(VIDEO_TITLE) like '%dark history%' then 'Dark History'
                when lower(VIDEO_TITLE) like '%dark horse nation%' then 'Dark Horse Nation'
                when lower(VIDEO_TITLE) like '%dead again%' then 'Dead Again'
                when lower(VIDEO_TITLE) like '%deadly wives%' then 'Deadly Wives'
                when lower(VIDEO_TITLE) like '%death saved my life%' then 'Death Saved My Life'
                when lower(VIDEO_TITLE) like '%history by the decade%' then 'Decade'
                when lower(VIDEO_TITLE) like '%deconstructing history%' then 'Deconstructing History'
                when lower(VIDEO_TITLE) like '%deep fried dynasty%' then 'Deep Fried Dynasty'
                when lower(VIDEO_TITLE) like '%detroit steel%' then 'Detroit Steel'
                when lower(VIDEO_TITLE) like '%detroit swat%' then 'Detroit SWAT'
                when lower(VIDEO_TITLE) like '%diettribe%' then 'DietTribe'
                when lower(VIDEO_TITLE) like '%digging for the truth%' or lower(VIDEO_TITLE) like '%digging for truth%' then 'Digging for Truth'
                when lower(VIDEO_TITLE) like '%digital addiction%' then 'Digital Addiction'
                when lower(VIDEO_TITLE) like '%dirty rotten cleaners%' then 'Dirty Rotten Cleaners'
                when lower(VIDEO_TITLE) like '%dog the bounty hunter%' then 'Dog The Bounty Hunter'
                when lower(VIDEO_TITLE) like '%dogfights%' then 'Dogfights'
                when lower(VIDEO_TITLE) like '%dogs of war%' then 'Dogs of War'
                when lower(VIDEO_TITLE) like '%donnie loves jenny%' then 'Donnie Love Jenny'
                when lower(VIDEO_TITLE) like '%don''t trust andrew mayne%' then 'Don''t Trust Andrew Mayne'
                when lower(VIDEO_TITLE) like '%double divas%' then 'Double Divas'
                when lower(VIDEO_TITLE) like '%down east dickering%' then 'Down East Dickering'
                when lower(VIDEO_TITLE) like '%drawn history%' then 'Drawn History'
                when lower(VIDEO_TITLE) like '%duck dynasty%' then 'Duck Dynasty'
                when lower(VIDEO_TITLE) like '%eating history%' then 'Eating History'
                when lower(VIDEO_TITLE) like '%engineering an empire%' then 'Engineering an Empire'
                when lower(VIDEO_TITLE) like '%engineering disasters%' then 'Engineering Disasters'
                when lower(VIDEO_TITLE) like '%epic ink%' then 'Epic Ink'
                when lower(VIDEO_TITLE) like '%escaping polygamy%' then 'Escaping Polygamy'
                when lower(VIDEO_TITLE) like '%evel live%' then 'Evel Live'
                when lower(VIDEO_TITLE) like '%every woman counts%' then 'Every Woman Counts'
                when lower(VIDEO_TITLE) like '%expedition bermuda triangle%' then 'Expedition Bermuda Triangle'
                when lower(VIDEO_TITLE) like '%extreme builds%' then 'Extreme Builds'
                when lower(VIDEO_TITLE) like '%extreme trains%' then 'Extreme Trains'
                when lower(VIDEO_TITLE) like '%extreme unboxing%' then 'Extreme Unboxing'
                when lower(VIDEO_TITLE) like '%families that fed america%' then 'Families that fed America'
                when lower(VIDEO_TITLE) like '%fashionably late with rachel zoe%' then 'Fashionably Late with Rachel Zoe'
                when lower(VIDEO_TITLE) like '%fasten your seatbelt%' then 'Fasten Your Seatbelt'
                when lower(VIDEO_TITLE) like '%fear: buired alive%' then 'Fear: Buired Alive'
                when lower(VIDEO_TITLE) like '%fempire moments%' then 'Fempire Moments'
                when lower(VIDEO_TITLE) like '%first 48%' or lower(VIDEO_TITLE) like '%homicide squad%' then 'First 48'
                when lower(VIDEO_TITLE) like '%first blood%' then 'First Blood'
                when lower(VIDEO_TITLE) like '%fit to fat to fit%' then 'Fit to Fat to Fit'
                when lower(VIDEO_TITLE) like '%fix this yard%' then 'Fix This Yard'
                when lower(VIDEO_TITLE) like '%flipped off%' then 'Flipped Off'
                when lower(VIDEO_TITLE) like '%flipping boston%' then 'Flipping Boston'
                when lower(VIDEO_TITLE) like '%flipping san diego%' then 'Flipping San Diego'
                when lower(VIDEO_TITLE) like '%flipping vegas%' then 'Flipping Vegas'
                when lower(VIDEO_TITLE) like '%forensic factor%' then 'Forensic Factor'
                when lower(VIDEO_TITLE) like '%forged in fire%' then 'Forged in Fire'
                when lower(VIDEO_TITLE) like '%four of a kind%' then 'Four of a Kind'
                when lower(VIDEO_TITLE) like '%frasier%' then 'Frasier'
                when lower(VIDEO_TITLE) like '%full documentary%' then 'Full Documentary'
                when lower(VIDEO_TITLE) like '%full metal jousting%' then 'Full Metal Jousting'
                when lower(VIDEO_TITLE) like '%full movie%' or lower(VIDEO_TITLE) like '%wanda durant story%' or lower(VIDEO_TITLE) like '%college admissions scandal%' or lower(VIDEO_TITLE) like '%james van der beek%' or lower(VIDEO_TITLE) like '%taken in broad daylight%' or lower(VIDEO_TITLE) like '%full mini-movie%' then 'Full Movie'
                when lower(VIDEO_TITLE) like '%gangland undercover%' then 'Gangland Undercover'
                when lower(VIDEO_TITLE) like '%gangsters america''s most evil%' or lower('video_title') like '%gangsters: america''s most evil%' then 'Gangsters: America''s Most Evil'
                when lower(VIDEO_TITLE) like '%gene simmons family jewels%' then 'Gene Simmons Family Jewels'
                when lower(VIDEO_TITLE) like '%get swank''d%' then 'Get Swank''d'
                when lower(VIDEO_TITLE) like '%ghost hunters%' then 'Ghost Hunters'
                when lower(VIDEO_TITLE) like '%ghost inside my child%' then 'Ghost Inside My Child'
                when lower(VIDEO_TITLE) like '%the ghost inside my house%' then 'Ghost Inside My House'
                when lower(VIDEO_TITLE) like '%glam masters%' then 'Glam Masters'
                when lower(VIDEO_TITLE) like '%god, guns & automobiles%' then 'God, Guns & Automobiles'
                when lower(VIDEO_TITLE) like '%godfather of pittsburgh%' then 'Godfather of Pittsburgh'
                when lower(VIDEO_TITLE) like '%going si-ral%' then 'Going Si-ral'
                when lower(VIDEO_TITLE) like '%grant:%' then 'Grant'
                when lower(VIDEO_TITLE) like '%great escapes with morgan freeman%' then 'Great Escapes with Morgan Freeman'
                when lower(VIDEO_TITLE) like '%great minds with dan harmon%' then 'Great Minds with Dan Harmon'
                when lower(VIDEO_TITLE) like '%great wild north%' then 'Great Wild North'
                when lower(VIDEO_TITLE) like '%growing up gotti%' then 'Growing Up Gotti'
                when lower(VIDEO_TITLE) like '%growing up supermodel%' or lower(VIDEO_TITLE) like '%growing up super model%' then 'Growing Up Supermodel'
                when lower(VIDEO_TITLE) like '%hamilton: building america%' then 'Hamilton: Building America'
                when lower(VIDEO_TITLE) like '%harry & meghan%' then 'Harry & Meghan'
                when lower(VIDEO_TITLE) like '%hatfields & mccoys%' then 'Hatfields & McCoys'
                when lower(VIDEO_TITLE) like '%hero ink%' then 'Hero Ink'
                when lower(VIDEO_TITLE) like '%greatest mysteries: solved%' then 'HGMS'
                when lower(VIDEO_TITLE) like '%hider in my house%' then 'Hider in my house'
                when lower(VIDEO_TITLE) like '%history at home%' then 'History at Home'
                when lower(VIDEO_TITLE) like '%history now%' then 'History NOW'
                when lower(VIDEO_TITLE) like '%history of soccer%' then 'History of Soccer'
                when lower(VIDEO_TITLE) like '%history of the holidays%' then 'History of the Holidays'
                when lower(VIDEO_TITLE) like '%history remade%' then 'History Remade'
                when lower(VIDEO_TITLE) like '%history vault exclusive%' then 'History Vault Exclusive'
                when lower(VIDEO_TITLE) like '%greatest mysteries%' or lower(VIDEO_TITLE) like '%history''s greatest mysteries%' then 'History''s Greatest Mysteries'
                when lower(VIDEO_TITLE) like '%hoarders%' then 'Hoarders'
                when lower(VIDEO_TITLE) like '%how the earth was made%' then 'How the Earth was Made'
                when lower(VIDEO_TITLE) like '%states got their shapes%' then 'How the States Got Their Shapes'
                when lower(VIDEO_TITLE) like '%human weapon%' then 'Human Weapon'
                when lower(VIDEO_TITLE) like '%hunting hilter%' then 'Hunting Hilter'
                when lower(VIDEO_TITLE) like '%hunting isis%' then 'Hunting ISIS'
                when lower(VIDEO_TITLE) like '%hurricane katrina%' then 'Hurricane Katrina'
                when lower(VIDEO_TITLE) like '%hustle & tow%' then 'Hustle & Tow'
                when lower(VIDEO_TITLE) like '%i survived a crime%' or  lower(VIDEO_TITLE) = 'woman''s ex attacked by her father when trying to storm their home | a&e' or  lower(VIDEO_TITLE) = 'real-life hero stops two carjackings at fast food restaurant | a&e' or lower(VIDEO_TITLE) =
                'bulletproof vest saves clerk''s life during shootout | a&e' or lower(VIDEO_TITLE) = 'raging driver''s shocking “self-defense” move | a&e' then 'I Survived a Crime'
                when lower(VIDEO_TITLE) like '%i survived%' and lower(VIDEO_TITLE) not like '%i survived a crime%' then 'I Survived'
                when lower(VIDEO_TITLE) like '%i was there%' then 'I Was There'
                when lower(VIDEO_TITLE) like '%ice road truckers%' or lower(VIDEO_TITLE) like '%ice roade truckers%' then 'Ice Road Truckers'
                when lower(VIDEO_TITLE) like '%in search of%' and lower(VIDEO_TITLE) like '%aliens%' then 'In Search Of Aliens'
                when lower(VIDEO_TITLE) like '%in search of%' and lower(VIDEO_TITLE) like '%christmas%' then 'In Search Of Christmas'
                when lower(VIDEO_TITLE) like '%in search of%' and lower(VIDEO_TITLE) not like '%aliens%' and lower(VIDEO_TITLE) not like '%christmas%' then 'In Search Of'
                when lower(VIDEO_TITLE) like '%infamous cases%' then 'Infamous Cases'
                when lower(VIDEO_TITLE) like '%infamous killers%' then 'Infamous Killers'
                when lower(VIDEO_TITLE) like '%inmate to roommate%' then 'Inmate to Roommate'
                when lower(VIDEO_TITLE) like '%interrogation cam%' and lower(VIDEO_TITLE) not like '%interrogation raw%' then 'Interrogation Cam'
                when lower(VIDEO_TITLE) like '%interrogation raw%' then 'Interrogation Raw'
                when lower(VIDEO_TITLE) like '%interrogators%' or lower(VIDEO_TITLE) like '%the interrogators%' then 'Interrogators'
                when lower(VIDEO_TITLE) like '%intervention%' then 'Intervention'
                when lower(VIDEO_TITLE) like '%invention usa%' then 'Invention USA'
                when lower(VIDEO_TITLE) like '%invisible monsters%' then 'Invisible Monsters'
                when lower(VIDEO_TITLE) like '%iron & fire%' then 'Iron & Fire'
                when lower(VIDEO_TITLE) like '%irt deadliest roads%' then 'IRT Deadliest Roads'
                when lower(VIDEO_TITLE) like '%jacked: auto theft task force%' then 'Jacked: Auto Theft Task Force'
                when lower(VIDEO_TITLE) like '%janet jackson%' or lower(VIDEO_TITLE) like '%janet%' then 'Janet Jackson'
                when lower(VIDEO_TITLE) like '%jesus: his life%' then 'Jesus: His Life'
                when lower(VIDEO_TITLE) like '%jfk declassified%' then 'JFK Declassified'
                when lower(VIDEO_TITLE) like '%join or die with craig ferguson%' then 'Join or Die with Craig Ferguson'
                when lower(VIDEO_TITLE) like '%jump!%' then 'Jump!'
                when lower(VIDEO_TITLE) like '%jurassic fight club%' then 'Jurassic Fight Club'
                when lower(VIDEO_TITLE) like '%kansas city swat%' then 'Kansas City SWAT'
                when lower(VIDEO_TITLE) like '%kids behind%' then 'Kids Behind Bars'
                when lower(VIDEO_TITLE) like '%kids history%' then 'Kids History'
                when lower(VIDEO_TITLE) like '%killer cases%' then 'Killer Cases'
                when lower(VIDEO_TITLE) like '%killer confessions%' and lower(VIDEO_TITLE) not like '%killer cases%' and lower(VIDEO_TITLE) not like '%killer speaks%'then 'Killer Confessions'
                when lower(VIDEO_TITLE) like '%kim of queens%' then 'Kim of Queens'
                when lower(VIDEO_TITLE) like '%king of cars%' then 'King of Cars'
                when lower(VIDEO_TITLE) like '%kings of pain%' then 'Kings of Pain'
                when lower(VIDEO_TITLE) like '%kiss bang love%' then 'Kiss Bang Love'
                when lower(VIDEO_TITLE) like '%knight fight%' then 'Knight Fight'
                when lower(VIDEO_TITLE) like '%knightfall%' then 'Knightfall'
                when lower(VIDEO_TITLE) like '%l.a. burning%' or lower(VIDEO_TITLE) like '%the riots 25 years later%' then 'L.A. Burning'
                when lower(VIDEO_TITLE) like '%lachey''s bar%' then 'Lachey''s Bar'
                when lower(VIDEO_TITLE) like '%lady hoggers%' then 'Lady Hoggers'
                when lower(VIDEO_TITLE) like '%last days of the nazis%' then 'Last Days of the Nazis'
                when lower(VIDEO_TITLE) like '%leah remini%' then 'Leah Remini'
                when lower(VIDEO_TITLE) like '%leave it to geege%' then 'Leave it to Geege'
                when lower(VIDEO_TITLE) like '%leepu%' then 'Leepu & Pitbull'
                when lower(VIDEO_TITLE) like '%legend of superstition%' or lower(VIDEO_TITLE) like '%legend of the superstition%' or lower(VIDEO_TITLE) like '%legend of superstition mountains%' then 'Legend of Superstition Mountains'
                when lower(VIDEO_TITLE) like '%life after people%' then 'Life After People'
                when lower(VIDEO_TITLE) like '%lifetime movie moments%' or lower(VIDEO_TITLE) like '%lifetime movie moment%' then 'Lifetime Movie Moments'
                when lower(VIDEO_TITLE) like '%limo wars%' then 'Limo Wars'
                when lower(VIDEO_TITLE) like '%little weddings%' then 'Little Weddings'
                when lower(VIDEO_TITLE) like '%little women: atlanta%' then 'Little Women: Atlanta'
                when lower(VIDEO_TITLE) like '%little women: dallas%' then 'Little Women: Dallas'
                when lower(VIDEO_TITLE) like '%little women: la%' then 'Little Women: LA'
                when lower(VIDEO_TITLE) like '%little women: ny%' then 'Little Women: NY'
                when lower(VIDEO_TITLE) like '%little women%' then 'Little Women'
                when lower(VIDEO_TITLE) like '%live pd%' then 'Live PD'
                when lower(VIDEO_TITLE) like '%live rescue%' then 'Live Rescue'
                when lower(VIDEO_TITLE) like '%lone star lady%' then 'Lone Star Lady'
                when lower(VIDEO_TITLE) like '%lone star restoration%' then 'Lone Star Restoration'
                when lower(VIDEO_TITLE) like '%lost gold of the aztecs%' or lower(VIDEO_TITLE) like '%lost gold of aztecs%' then 'Lost Gold of the Aztecs'
                when lower(VIDEO_TITLE) like '%lost gold of wwii%' or lower(VIDEO_TITLE) like '%lost gold of world%' then 'Lost Gold of WWII'
                when lower(VIDEO_TITLE) like '%lost history%' then 'Lost History'
                when lower(VIDEO_TITLE) like '%lost in transmission%' then 'Lost in Transmission'
                when lower(VIDEO_TITLE) like '%lost worlds%' then 'Lost Worlds'
                when lower(VIDEO_TITLE) like '%love at first flight%' then 'Love at First Flight'
                when lower(VIDEO_TITLE) like '%love prison%' then 'Love Prison'
                when lower(VIDEO_TITLE) like '%mafs%' or lower(VIDEO_TITLE) like '%married at first sight%' then 'MAFS'
                when lower(VIDEO_TITLE) like '%making a model%' then 'Making a Model'
                when lower(VIDEO_TITLE) like '%man vs. child%' or lower(VIDEO_TITLE) like '%man vs child%' then 'Man vs Child'
                when lower(VIDEO_TITLE) like '%man vs history%' then 'Man Vs History'
                when lower(VIDEO_TITLE) like '%manhunters%' then 'Manhunters'
                when lower(VIDEO_TITLE) like '%mankind%' then 'Mankind'
                when lower(VIDEO_TITLE) like '%marrying millions%' then 'Marrying Millions'
                when lower(VIDEO_TITLE) like '%meet marry murder%'  then 'Meet Marry Murder'
                when lower(VIDEO_TITLE) like '%mega disaster%' then 'Mega Disasters'
                when lower(VIDEO_TITLE) like '%mega movers%' then 'Mega Movers'
                when lower(VIDEO_TITLE) like '%men who built america%' then 'Men Who Built America'
                when lower(VIDEO_TITLE) like '%mikie saves the date%' then 'Mikie Saves the Date'
                when lower(VIDEO_TITLE) like '%million dollar genius%' then 'Million Dollar Genius'
                when lower(VIDEO_TITLE) like '%milwaukee blacksmith%' then 'Milwaukee Blacksmith'
                when lower(VIDEO_TITLE) like '%missing in alaska%' then 'Missing in Alaska'
                when lower(VIDEO_TITLE) like '%mississippi men%' then 'Mississippi Men'
                when lower(VIDEO_TITLE) like '%mobsters%' then 'Mobsters'
                when lower(VIDEO_TITLE) like '%modern dads%' then 'Modern Dads'
                when lower(VIDEO_TITLE) like '%modern marvels%' then 'Modern Marvels'
                when lower(VIDEO_TITLE) like '%monster in my family%' then 'Monster in My Family'
                when lower(VIDEO_TITLE) like '%monster in-laws%' then 'Monster In-Laws'
                when lower(VIDEO_TITLE) like '%monsterquest%' then 'MonsterQuest'
                when lower(VIDEO_TITLE) like '%montecito%' then 'Montecito'
                when lower(VIDEO_TITLE) like '%monument guys%' then 'Monument Guys'
                when lower(VIDEO_TITLE) like '%more like sister%' then 'More Like Sisters'
                when lower(VIDEO_TITLE) like '%more power%' then 'More Power'
                when lower(VIDEO_TITLE) like '%most daring%' then 'Most Daring'
                when lower(VIDEO_TITLE) like '%mountain men%' then 'Mountain Men'
                when lower(VIDEO_TITLE) like '%mountaintop moments%' then 'Mountaintop Moments'
                when lower(VIDEO_TITLE) like '%mounted in alaska%' then 'Mounted in Alaska'
                when lower(VIDEO_TITLE) like '%music factory%' then 'Ms. Ts Music Factory'
                when lower(VIDEO_TITLE) like '%museum men%' then 'Museum Men'
                when lower(VIDEO_TITLE) like '%my killer body%' then 'My Killer Body'
                when lower(VIDEO_TITLE) like '%nature gone wild%' then 'Nature Gone Wild'
                when lower(VIDEO_TITLE) like '%navajo code talkers%' then 'Navajo Code Talkers'
                when lower(VIDEO_TITLE) like '%navy seals%' then 'Navy Seals'
                when lower(VIDEO_TITLE) like '%neighbor in the window%' then 'Neighbor in the Window'
                when lower(VIDEO_TITLE) like '%neighborhood wars%' then 'Neighborhood Wars'
                when lower(VIDEO_TITLE) like '%neighbors with benefits%' then 'Neighbors with Benefits'
                when lower(VIDEO_TITLE) like '%night class%' then 'Night Class'
                when lower(VIDEO_TITLE) like '%nightwatch%' then 'Nightwatch'
                when lower(VIDEO_TITLE) like '%nine lives of ozzy osbourne%' then 'Nine Lives of Ozzy Osbourne'
                when lower(VIDEO_TITLE) like '%nostradamus & the end of time%' then 'Nostradamus & the End of Time'
                when lower(VIDEO_TITLE) like '%nostradamus effect%' then 'Nostradamus Effect'
                when lower(VIDEO_TITLE) like '%obsessed%' then 'Obsessed'
                when lower(VIDEO_TITLE) like '%one born every minute%' then 'One Born Every Minute'
                when lower(VIDEO_TITLE) like '%only in america%' then 'Only In America'
                when lower(VIDEO_TITLE) like '%outback hunters%' then 'Outback Hunters'
                when lower(VIDEO_TITLE) like '%outlaw chronicles%' then 'Outlaw Chronicles'
                when lower(VIDEO_TITLE) like '%ozzy & jack''s world detour%' then 'Ozzy & Jack''s World Detour'
                when lower(VIDEO_TITLE) like '%panic 9-1-1%' then 'Panic 9-1-1'
                when lower(VIDEO_TITLE) like '%paranormal state%' then 'Paranormal State'
                when lower(VIDEO_TITLE) like '%parking wars%' then 'Parking Wars'
                when lower(VIDEO_TITLE) like '%patton 360%' then 'Patton 360'
                when lower(VIDEO_TITLE) like '%pawn stars do america%' and lower(VIDEO_TITLE) not like '%pawn stars%' then 'Pawn Stars Do America'
                when lower(VIDEO_TITLE) like '%pawn stars%' then 'Pawn Stars'
                when lower(VIDEO_TITLE) like '%pd stories%' then 'PD Stories'
                when lower(VIDEO_TITLE) like '%pearl harbor%' then 'Pearl Harbor'
                when lower(VIDEO_TITLE) like '%phrogging: hider in my house%' or lower(VIDEO_TITLE) like '%phrogging%' then 'Phrogging'
                when lower(VIDEO_TITLE) like '%pirate treasure of the knights templar%' then 'Pirate Treasure of the Knights Templar'
                when lower(VIDEO_TITLE) like '%planet egypt%' then 'Planet Egypt'
                when lower(VIDEO_TITLE) like '%power & ice%' then 'Power & Ice'
                when lower(VIDEO_TITLE) like '%preachers'' daughters%' or lower(VIDEO_TITLE) like '%preachers daughters%' or lower(VIDEO_TITLE) like '%preacher''s daughters%' then 'Preachers'' Daughters'
                when lower(VIDEO_TITLE) like '%presidents at war%' then 'Presidents at War'
                when lower(VIDEO_TITLE) like '%pretty wicked moms%' then 'Pretty Wicked Moms'
                when lower(VIDEO_TITLE) like '%prison police%' then 'Prison Police'
                when lower(VIDEO_TITLE) like '%prison wives club%' then 'Prison Wives Club'
                when lower(VIDEO_TITLE) like '%project blue book%' then 'Project Blue Book'
                when lower(VIDEO_TITLE) like '%project runway %' then 'Project Runway '
                when lower(VIDEO_TITLE) like '%proof is out there%' then 'Proof Is Out There'
                when lower(VIDEO_TITLE) like '%psychic kids%' then 'Psychic Kids'
                when lower(VIDEO_TITLE) like '%psychic tia%' then 'Psychic Tia'
                when lower(VIDEO_TITLE) like '%raising asia%' then 'Raising Asia'
                when lower(VIDEO_TITLE) like '%ranchlands%' then 'Ranchlands'
                when lower(VIDEO_TITLE) like '%rap game%' then 'Rap Game'
                when lower(VIDEO_TITLE) like '%reading for roots%' then 'Reading for Roots'
                when lower(VIDEO_TITLE) like '%real crime%' then 'Real Crime'
                when lower(VIDEO_TITLE) like '%real deal%' then 'Real Deal'
                when lower(VIDEO_TITLE) like '%remembering 9/11%' then 'Remembering 9/11'
                when lower(VIDEO_TITLE) like '%rescue cam%' then 'Rescue Cam'
                when lower(VIDEO_TITLE) like '%revelation%' then 'Revelation'
                when lower(VIDEO_TITLE) like '%rivermen%' then 'Rivermenn'
                when lower(VIDEO_TITLE) like '%road hauks%' then 'Road Hauks'
                when lower(VIDEO_TITLE) like '%road to 9/11%' then 'Road to 9/11'
                when lower(VIDEO_TITLE) like '%road wars%' then 'Road Wars'
                when lower(VIDEO_TITLE) like '%roanoke%' then 'Roanoke'
                when lower(VIDEO_TITLE) like '%rodeo girls%' then 'Rodeo Girls'
                when lower(VIDEO_TITLE) like '%rise and fall of an empire%' then 'Rome: Rise and Fall of an Empire'
                when lower(VIDEO_TITLE) like '%rookies: jefferson%' then 'Rookies: Jefferson'
                when lower(VIDEO_TITLE) like '%roots%' then 'Roots'
                when lower(VIDEO_TITLE) like '%runaway squad%' then 'Runaway Squad'
                when lower(VIDEO_TITLE) like '%search for the lost giants%' or lower(VIDEO_TITLE) like '%search for lost giants%' then 'Search for the Lost Giants'
                when lower(VIDEO_TITLE) like '%seatbelt psychic%' or lower(VIDEO_TITLE) like '%seat belt psychic%' then 'Seatbelt Psychic'
                when lower(VIDEO_TITLE) like '%secret restoration%' then 'Secret Restoration'
                when lower(VIDEO_TITLE) like '%secrets of ancient egypt%' then 'Secrets of Ancient Egypt'
                when lower(VIDEO_TITLE) like '%secrets of playboy%' and lower(VIDEO_TITLE) not like '%secrets of polygamy%' then 'Secrets of Playboy'
                when lower(VIDEO_TITLE) like '%secrets of polygamy%' then 'Secrets of Polygamy'
                when lower(VIDEO_TITLE) like '%sell this house%' then 'Sell This House'
                when lower(VIDEO_TITLE) like '%seven year switch%' then 'Seven Year Switch'
                when lower(VIDEO_TITLE) like '%sexy beasts%' then 'Sexy Beasts'
                when lower(VIDEO_TITLE) like '%shark wranglers%' then 'Shark Wranglers'
                when lower(VIDEO_TITLE) like '%shipping wars%' or lower(VIDEO_TITLE) like '%pirate-themed, handcrafted playground haul%' or lower(VIDEO_TITLE) like '%finding one-of-a-kind handcrafted bamboo fishing rods%' then 'Shipping Wars'
                when lower(VIDEO_TITLE) like '%shootout!%' then 'Shootout!'
                when lower(VIDEO_TITLE) like '%skinwalker ranch%' then 'Skinwalker Ranch'
                when lower(VIDEO_TITLE) like '%sleeping with a killer%' then 'Sleeping with a Killer'
                when lower(VIDEO_TITLE) like '%smartest guy in the room%' then 'Smartest Guy in the Room'
                when lower(VIDEO_TITLE) like '%smile%' then 'Smile'
                when lower(VIDEO_TITLE) like '%snooki%' then 'Snooki & Jionni’s Short Flip'
                when lower(VIDEO_TITLE) like '%so sharp%' then 'So Sharp'
                when lower(VIDEO_TITLE) like '%sons of liberty%' then 'Sons of Liberty'
                when lower(VIDEO_TITLE) like '%sound smart%' then 'Sound Smart'
                when lower(VIDEO_TITLE) like '%southie rules%' then 'Southie Rules'
                when lower(VIDEO_TITLE) like '%stan lee''s superhumans%' or lower(VIDEO_TITLE) like '%superhumans%' then 'Stan Lee''s Superhumans'
                when lower(VIDEO_TITLE) like '%step it up%' then 'Step It Up'
                when lower(VIDEO_TITLE) like '%storage wars%' then 'Storage Wars'
                when lower(VIDEO_TITLE) like '%stories from the longest road%' then 'Stories from the Longest Road'
                when lower(VIDEO_TITLE) like '%stories of horsepower%' then 'Stories of Horsepower'
                when lower(VIDEO_TITLE) like '%streets of compton%' then 'Streets of Compton'
                when lower(VIDEO_TITLE) like '%strongest man%' or lower(VIDEO_TITLE) like '%strongest men%' then 'Strongest Man'
                when lower(VIDEO_TITLE) like '%studio rescue%' then 'Studio Rescue'
                when lower(VIDEO_TITLE) like '%supernanny%' then 'Supernanny'
                when lower(VIDEO_TITLE) like '%surviving r. kelly%' then 'Surviving R. Kelly'
                when lower(VIDEO_TITLE) like '%swamp mysteries%' then 'Swamp Mysteries with Troy Landry'
                when lower(VIDEO_TITLE) like '%swamp people%' then 'Swamp People'
                when lower(VIDEO_TITLE) like '%taking the stand%' then 'Taking the Stand'
                when lower(VIDEO_TITLE) like '%teenage newlyweds%' then 'Teenage Newlyweds'
                when lower(VIDEO_TITLE) like '%tesla files%' then 'Tesla Files'
                when lower(VIDEO_TITLE) like '%texas rising%' then 'Texas Rising'
                when lower(VIDEO_TITLE) like '%the "i do" diaries%' then 'The "I Do" Diaries'
                when lower(VIDEO_TITLE) like '%the big fat wedding walks!%' or lower(VIDEO_TITLE) like '%the big fat wedding walk%' then 'The Big Fat Wedding Walk'
                when lower(VIDEO_TITLE) like '%the butcher%' then 'The Butcher'
                when lower(VIDEO_TITLE) like '%the case i can"t forget%' or lower(VIDEO_TITLE) like '%the case i%' then 'The Case I Can"t Forget'
                when lower(VIDEO_TITLE) like '%the christmas truth%' then 'The Christmas Truth'
                when lower(VIDEO_TITLE) like '%the civil war in color%' then 'The Civil War in Color'
                when lower(VIDEO_TITLE) like '%the clark sisters%' then 'The Clark Sisters'
                when lower(VIDEO_TITLE) like '%the curse of civil war gold%' then 'The Curse of Civil War Gold'
                when lower(VIDEO_TITLE) like '%the eleven%' then 'The Eleven'
                when lower(VIDEO_TITLE) like '%the enfield haunting%' then 'The Enfield Haunting'
                when lower(VIDEO_TITLE) like '%the engineering that built the world%' or lower(VIDEO_TITLE) like '%engineering that built%' then 'The Engineering that Built the World'
                when lower(VIDEO_TITLE) like '%food that built america%' then 'The Food That Built America'
                when lower(VIDEO_TITLE) like '%the governor''s wife%' then 'The Governor''s Wife'
                when lower(VIDEO_TITLE) like '%the haunting of%' then 'The Haunting of'
                when lower(VIDEO_TITLE) like '%the hidden tapes%' then 'The Hidden Tapes'
                when lower(VIDEO_TITLE) like '%the hunt%' then 'The Hunt'
                when lower(VIDEO_TITLE) like '%hunt for the zodiac killer%' then 'The Hunt for the Zodiac Killer'
                when lower(VIDEO_TITLE) like '%the interrogators%' then 'The Interrogators'
                when lower(VIDEO_TITLE) like '%the jacksons%' then 'The Jacksons'
                when lower(VIDEO_TITLE) like '%the killer speaks%' then 'The Killer Speaks'
                when lower(VIDEO_TITLE) like '%the killing season%' then 'The Killing Season'
                when lower(VIDEO_TITLE) like '%the legend of shelby the swamp man%' then 'The Legend of Shelby the Swamp Man'
                when lower(VIDEO_TITLE) like '%the lowe files%' then 'The Lowe Files'
                when lower(VIDEO_TITLE) like '%the machines that built america%' then 'The Machines That Built America'
                when lower(VIDEO_TITLE) like '%the murder of laci peterson%' then 'The Murder of Laci Peterson'
                when lower(VIDEO_TITLE) like '%the redeemed%' then 'The Redeemed'
                when lower(VIDEO_TITLE) like '%the return of shelby the swamp man%' then 'The Return of Shelby the Swamp Man'
                when lower(VIDEO_TITLE) like '%the returned%' then 'The Returned'
                when lower(VIDEO_TITLE) like '%the revolution%' then 'The Revolution'
                when lower(VIDEO_TITLE) like '%the road to freedom%' then 'The Road to Freedom'
                when lower(VIDEO_TITLE) like '%the secret history of the civil war%' then 'The Secret History of the Civil War'
                when lower(VIDEO_TITLE) like '%the selection:%' then 'The Selection'
                when lower(VIDEO_TITLE) like '%sisterhood%' then 'The Sisterhood'
                when lower(VIDEO_TITLE) like '%the spirit that built america%' then 'The Spirit That Built America'
                when lower(VIDEO_TITLE) like '%the titans that built america%' then 'The Titans that built America'
                when lower(VIDEO_TITLE) like '%the toe bro%' then 'The Toe Bro'
                when lower(VIDEO_TITLE) like '%the top secret tapes%' then 'The Top Secret Tapes'
                when lower(VIDEO_TITLE) like '%the universe%' then 'The Universe'
                when lower(VIDEO_TITLE) like '%the woodsmen%' then 'The Woodsmen'
                when lower(VIDEO_TITLE) like '%the world wars%' then 'The World Wars'
                when lower(VIDEO_TITLE) like '%this day in history%' then 'This Day In History'
                when lower(VIDEO_TITLE) like '%this time next year%' then 'This Time Next Year'
                when lower(VIDEO_TITLE) like '%this week in history%' then 'This Week in History'
                when lower(VIDEO_TITLE) like '%tiny house hunting%' then 'Tiny House Hunting'
                when lower(VIDEO_TITLE) like '%top gear%' then 'Top Gear'
                when lower(VIDEO_TITLE) like '%top shot%' then 'Top Shot'
                when lower(VIDEO_TITLE) like '%truck night in america%' then 'Truck Night in America'
                when lower(VIDEO_TITLE) like '%true monsters%' then 'True Monsters'
                when lower(VIDEO_TITLE) like '%tulsa burning%' then 'Tulsa Burning'
                when lower(VIDEO_TITLE) like '%tupac%' then 'Tupac'
                when lower(VIDEO_TITLE) like '%tuskegee airmen%' then 'Tuskegee Airmen'
                when lower(VIDEO_TITLE) like '%ufo hunters%' then 'UFO Hunters'
                when lower(VIDEO_TITLE) like '%ultimate guide to the presidents%' then 'Ultimate Guide to the Presidents'
                when lower(VIDEO_TITLE) like '%ultimate soldier challenge%' then 'Ultimate Soldier Challenge'
                when lower(VIDEO_TITLE) like '%unforgettable%' then 'Unforgettable'
                when lower(VIDEO_TITLE) like '%unidentified%' then 'Unidentified'
                when lower(VIDEO_TITLE) like '%united we drive%' then 'United We Drive'
                when lower(VIDEO_TITLE) like '%unxplained%' then 'UnXplained'
                when lower(VIDEO_TITLE) like '%very superstitious with george lopez%' then 'Very Superstitious with George Lopez'
                when lower(VIDEO_TITLE) like '%vikings%' then 'Vikings'
                when lower(VIDEO_TITLE) like '%voices magnified%' then 'Voices Magnified'
                when lower(VIDEO_TITLE) like '%wahlburgers%' then 'Wahlburgers?'
                when lower(VIDEO_TITLE) like '%warfighters%' then 'Warfighters'
                when lower(VIDEO_TITLE) like '%warriors%' then 'Warriors'
                when lower(VIDEO_TITLE) like '%washington%' then 'Washington'
                when lower(VIDEO_TITLE) like '%web originals%' then 'Web Originals'
                when lower(VIDEO_TITLE) like '%we''re the fugawis%' then 'We''re The Fugawis'
                when lower(VIDEO_TITLE) like '%what"s it worth%' then 'What"s It Worth?'
                when lower(VIDEO_TITLE) like '%when big things go wrong%' then 'When Big Things Go Wrong'
                when lower(VIDEO_TITLE) like '%why i ran%' then 'Why I Ran'
                when lower(VIDEO_TITLE) like '%wild transport%' then 'Wild Transport'
                when lower(VIDEO_TITLE) like '%wwe biography%' or lower(VIDEO_TITLE) like '%wwe bio%' then 'WWE Bio'
                when lower(VIDEO_TITLE) like '%wwe legends%' then 'WWE Legends'
                when lower(VIDEO_TITLE) like '%wwe rivals%' then 'WWE Rivals'
                when lower(VIDEO_TITLE) like '%most wanted treasures%' then 'WWE''s Most Wanted Treasures'
                when lower(VIDEO_TITLE) like '%wwi the first modern war%' then 'WWI The First Modern War'
                when lower(VIDEO_TITLE) like '%wwii in hd%' or lower(VIDEO_TITLE) like '%world war ii in hd%' then 'WWII In HD'
                when lower(VIDEO_TITLE) like '%wwe%' then 'WWE'
                when lower(VIDEO_TITLE) like '%jep & jessica%' or lower(VIDEO_TITLE) like '%jep and jessica%' then 'Jep & Jessica'
                when lower(VIDEO_TITLE) like '%he shed she shed%' then 'He Shed She Shed'
                when lower(VIDEO_TITLE) like '%l.a. detectives%' then 'L.A. Detectives'
                when lower(VIDEO_TITLE) like '%pawnography%' then 'Pawnography'
                when lower(VIDEO_TITLE) like '%waco siege%' then 'Waco Siege'
                when lower(VIDEO_TITLE) like '%black aviators%' then 'Black Aviators'
                when lower(VIDEO_TITLE) like '%surviving marriage%' then 'Surviving Marriage'
                when lower(VIDEO_TITLE) like '%the pop game%' then 'The Pop Game'
                when lower(VIDEO_TITLE) like '%prom queens%' then 'Prom Queens'
                when lower(VIDEO_TITLE) like '%kosher soul%' then 'Kosher Soul'
                when lower(VIDEO_TITLE) like '%laurieann gibson beyond the spotlight%' then 'Laurieann Gibson Beyond the Spotlight'
                when lower(VIDEO_TITLE) like '%heavy%' then 'Heavy'
                when lower(VIDEO_TITLE) like '%dance twins%' then 'Dance Twins'
                when lower(VIDEO_TITLE) like '%my haunted house%' then 'My Haunted House'
                when lower(VIDEO_TITLE) like '%my ghost story%' then 'My Ghost Story'
                when lower(VIDEO_TITLE) like '%mondo magic%' then 'Mondo Magic'
                when lower(VIDEO_TITLE) like '%ufo files%' then 'UFO Files'
                when lower(VIDEO_TITLE) like '%paranomal cops%' then 'Paranormal Cops'
                when lower(VIDEO_TITLE) like '%mysteryquest%' then 'MysteryQuest'
                when lower(VIDEO_TITLE) like '%haunted encounters%' then 'Haunted Encounters'
                when lower(VIDEO_TITLE) like '%stalked by a ghost%' then 'Stalked by a Ghost'
                when lower(VIDEO_TITLE) like '%haunted history%' then 'Haunted History'
                when lower(VIDEO_TITLE) like '%grace vs. abrams%' then 'Grace vs. Abrams'
                when lower(VIDEO_TITLE) like '%investigative reports%' then 'Investigative Reports'
                when lower(VIDEO_TITLE) like '%six on six:%' then 'Six on SIX:'
                when lower(VIDEO_TITLE) like '%doomsday%' then 'Doomsday'
                when lower(VIDEO_TITLE) like '%undercover caught on tape%' or lower(VIDEO_TITLE) like '%undercover: caught on tape%' then 'Undercover: Caught on Tape'
                when lower(VIDEO_TITLE) like '%evil up close%' then 'Evil Up Close'
                when lower(VIDEO_TITLE) like '%casey anthoy''s parents: the lie detector test%' or lower(VIDEO_TITLE) like '%casey anthony%' then 'Casey Anthony'
                when lower(VIDEO_TITLE) like '%women on death row%' then 'Women on Death Row'
                when lower(VIDEO_TITLE) like '%witness to murder%' then 'Witness to Murder'
                when lower(VIDEO_TITLE) like '%murder in the 21st%' then 'Murder in the 21st'
                when lower(VIDEO_TITLE) like '%my strange arrest%' then 'My Strange Arrest'
                when lower(VIDEO_TITLE) like '%the prison confessions of gypsy rose%' or lower(VIDEO_TITLE) like '%confessions of gypsy rose%' then 'Confessions of Gypsy Rose'
                when lower(VIDEO_TITLE) like '%i killed my bff%' then 'I Killed My BFF'
                when lower(VIDEO_TITLE) like '%deadly alibi%' and lower(VIDEO_TITLE) not like '%deadly wives%' then 'Deadly Alibi'
                when lower(VIDEO_TITLE) like '%prenup to murder%' then 'Prenup to Murder'
                when lower(VIDEO_TITLE) like '%last chance driving school%' then 'Last Chance Driving School'
                when lower(VIDEO_TITLE) like '%nightguard%' or lower(VIDEO_TITLE) like '%night guard%'and lower(VIDEO_TITLE) not like '%nightwatch%' then 'Nightguard'
                when lower(VIDEO_TITLE) like '%cars, cops, and criminals%' and lower(VIDEO_TITLE) not like '%car hunters%' then 'Cars, Cops, and Criminals'
                when lower(VIDEO_TITLE) like '%brit cops%' then 'Brit Cops'
                when lower(VIDEO_TITLE) like '%24 to life%'then '24 to Life'
                when lower(VIDEO_TITLE) like '%close encounters with evil%'then 'Close Encounters With Evil'
                when lower(VIDEO_TITLE) like '%female forces%'then 'Female Forces'
                when lower(VIDEO_TITLE) like '%my lover my killer%'then 'My Lover My Killer'
                when lower(VIDEO_TITLE) like '%police patrol%'then 'Police Patrol'
                when lower(VIDEO_TITLE) like '%sleeping with my murderer%'then 'Sleeping with my Murderer'
                when lower(VIDEO_TITLE) like '%the menendez murders: erik tells all%'then 'The Menendez Murders: Erik Tells All'
                when lower(VIDEO_TITLE) like '%watching the detectives%'then 'Watching the Detectives'
                when lower(VIDEO_TITLE) like '%when missing turns to murder%'then 'When Missing Turns to Murder'
                when lower(VIDEO_TITLE) like '%accused: guilty or innocent%' then 'Accused: Guilty or Innocent'
                ELSE 'other'
            END AS DOMO_SERIES_NAME
            FROM
                ({YT_metadata}) a
            LEFT JOIN ({ppl_table}) b
            ON EXTRACTED_PPL_ID = b.PROGRAM_ID
        """

    def map_PPL_data(
        self, aggregated_query: str, ppl_data: str, corrected_series: str
    ) -> str:
        """
        Function:
            * This function maps EXTRACTED_PPL_ID from extract_PPL_ID function to table.
            * This function also maps CORRECTED_SERIES details from Snowflake Table provided by Social Team.
        Params:
            * aggregated_query : input query
            * ppl_data : Output from extract_PPL_ID function
            * corrected_series : Table path for corrected series.
        Returns:
            * Query including corrected series.
        """
        return f"""
            SELECT 
                a.*,
                b.EXTRACTED_PPL_ID,
                COALESCE(
                    b.SERIES_ID,
                    CASE
                        WHEN c.CORRECTED_SERIES_ID IS NOT NULL THEN c.CORRECTED_SERIES_ID
                        WHEN b.series_name = b.domo_series_name THEN b.series_id
                        WHEN b.domo_series_name ILIKE CONCAT('%',b.series_name, '%') THEN b.series_id
                        ELSE '-1'
                    END 
                ) AS SERIES_ID,
                COALESCE(
                    b.SERIES_NAME,
                    CASE
                        WHEN c.CORRECTED_PPL_SERIES_NAME IS NOT NULL THEN c.CORRECTED_PPL_SERIES_NAME
                        WHEN b.series_name = b.domo_series_name THEN b.series_name
                        WHEN b.domo_series_name ILIKE CONCAT('%',b.series_name, '%') THEN b.series_name
                        ELSE 'other'
                    END 
                ) AS SERIES_NAME
            FROM ({aggregated_query}) a
            LEFT JOIN ({ppl_data}) b
            ON a.VIDEO_ID = b.VIDEO_ID
            LEFT JOIN ({corrected_series}) c
            ON b.DOMO_SERIES_NAME = c.DOMO_SERIES_NAME
        """

    def map_parent_series_id(self, query: str, ppl_table: str) -> str:
        """
        Function:
            * Maps PARENT_SERIES_ID using common SERIES_ID
        Params:
            * query : Input Query
            * metadata_table : Metadata table path
        Returns:
            * Mapped PARENT_SERIES_ID query
        """
        return f"""
            WITH ParentSeries AS (
                SELECT
                    b.series_ID,
                    b.PARENT_SERIES_ID,
                    ROW_NUMBER() OVER (PARTITION BY b.series_ID ORDER BY b.PARENT_SERIES_ID) AS rn
                FROM
                    {ppl_table} b
            )
            SELECT
                a.*, 
                ps.PARENT_SERIES_ID
            FROM
                ({query}) a
            LEFT JOIN 
                ParentSeries ps 
                ON a.SERIES_ID = ps.series_ID
                AND ps.rn = 1
        """

    def map_parent_series_name(self, query: str, ppl_series_table: str) -> str:
        """
        Function:
            * Maps PARENT_SERIES_NAME using common PARENT_SERIES_ID
        Params:
            * query : Input Query
            * metadata_table : Metadata table path
        Returns:
            * Mapped PARENT_SERIES_NAME query
        """
        return f"""
            WITH RankedPplSeries AS (
                SELECT
                    b.PARENT_SERIES_NAME,
                    b.PARENT_SERIES_ID,
                    ROW_NUMBER() OVER (PARTITION BY b.PARENT_SERIES_ID ORDER BY b.PARENT_SERIES_NAME) AS rn
                FROM
                    ({ppl_series_table}) b
                )
                SELECT
                    a.*,
                    rp.PARENT_SERIES_NAME
                FROM
                    ({query}) a
                LEFT JOIN RankedPplSeries rp
                    ON a.PARENT_SERIES_ID = rp.PARENT_SERIES_ID
                    AND rp.rn = 1
            """

    def map_brand_using_case_statement(self, query: str) -> str:
        """
        Function:
            * Maps BRAND in table using CHANNEL_NAME in case statement.

        Params:
            * query : Table in which BRAND is to be mapped (This table must contain CHANNEL_NAME for successful execution.)

        Returns:
            * Output table with BRAND mapped.
        """
        return f"""
            select
                *,
                case 
                    when (CHANNEL_NAME)='Mobsters' then 'A&E Brand'
                    when (CHANNEL_NAME)='WWE on A&E' then 'A&E Brand'
                    when (CHANNEL_NAME)='Storage Wars on A&E' then 'A&E Brand'
                    when (CHANNEL_NAME)='Live Rescue on A&E' then 'A&E Brand'
                    when (CHANNEL_NAME)='Live PD on A&E' then 'A&E Brand'
                    when (CHANNEL_NAME)='A&E' then 'A&E Brand'
                    when (CHANNEL_NAME)='I Survived a Crime' then 'A&E Brand'
                    when (CHANNEL_NAME)='Parking Wars' then 'A&E Brand'
                    when (CHANNEL_NAME)='Hoarders on A&E' then 'A&E Brand'
                    when (CHANNEL_NAME)='Crime 360' then 'A&E Brand'
                    when (CHANNEL_NAME)='Wahlburgers on A&E' then 'A&E Brand'
                    when (CHANNEL_NAME)='Born This Way on A&E' then 'A&E Brand'
                    when (CHANNEL_NAME)='Cold Case Files' then 'A&E Brand'
                    when (CHANNEL_NAME)='Intervention' then 'A&E Brand'
                    when (CHANNEL_NAME)='Shipping Wars on A&E' then 'A&E Brand'
                    when (CHANNEL_NAME)='Fasten Your Seatbelt' then 'A&E Brand'
                    when (CHANNEL_NAME)='A&E True Crime' then 'A&E Brand'
                    when (CHANNEL_NAME)='Crime & Investigation' then 'A&E Brand'
                    when (CHANNEL_NAME)='Celebrity Ghost Stories' then 'A&E Brand'
                    when (CHANNEL_NAME)='Nightwatch' then 'A&E Brand'
                    when (CHANNEL_NAME)='Nature Gone Wild' then 'A&E Brand'
                    when (CHANNEL_NAME)='60 Days In on A&E' then 'A&E Brand'
                    when (CHANNEL_NAME)='Court Cam' then 'A&E Brand'
                    when (CHANNEL_NAME)='Psychic Kids' then 'A&E Brand'
                    when (CHANNEL_NAME)='Billy the Exterminator on A&E' then 'A&E Brand'
                    when (CHANNEL_NAME)='Damien on A&E' then 'A&E Brand'
                    when (CHANNEL_NAME)='Behind Bars: Rookie Year on A&E' then 'A&E Brand'
                    when (CHANNEL_NAME)='DOG The Bounty Hunter on TV' then 'A&E Brand'
                    when (CHANNEL_NAME)='The First 48' then 'A&E Brand'
                    when (CHANNEL_NAME)='Duck Dynasty' then 'Emerging'
                    when (CHANNEL_NAME)='Home.Made.Nation' then 'Emerging'
                    when (CHANNEL_NAME)='LMN' then 'Emerging'
                    when (CHANNEL_NAME)='FYI Television Network' then 'Emerging'
                    when (CHANNEL_NAME)='Biography' then 'Emerging'
                    when (CHANNEL_NAME)='The UnXplained Zone' then 'Emerging'
                    when (CHANNEL_NAME)='Lifetime Movies' then 'Emerging'
                    when (CHANNEL_NAME)='The Unxplained Zone' then 'Emerging'
                    when (CHANNEL_NAME)='UnXplained Zone' then 'Emerging'
                    when (CHANNEL_NAME)='Tiny House Nation' then 'Emerging'
                    when (CHANNEL_NAME)='Unidentified: Inside America’s UFO Investigation' then 'Emerging'
                    when (CHANNEL_NAME)='Forged In Fire' then 'History Brand'
                    when (CHANNEL_NAME)='Swamp People Serpent Invasion' then 'History Brand'
                    when (CHANNEL_NAME)='Alone on HISTORY' then 'History Brand'
                    when (CHANNEL_NAME)='HISTORY' then 'History Brand'
                    when (CHANNEL_NAME)='Ancient Aliens: Out of This World Moments' then 'History Brand'
                    when (CHANNEL_NAME)='Military Heroes' then 'History Brand'
                    when (CHANNEL_NAME)='Knightfall' then 'History Brand'
                    when (CHANNEL_NAME)='Ax Men on History' then 'History Brand'
                    when (CHANNEL_NAME)='The Legend of Shelby the Swamp Man' then 'History Brand'
                    when (CHANNEL_NAME)='American Pickers' then 'History Brand'
                    when (CHANNEL_NAME)='The Universe' then 'History Brand'
                    when (CHANNEL_NAME)='MonsterQuest' then 'History Brand'
                    when (CHANNEL_NAME)='Pawn Stars on History' then 'History Brand'
                    when (CHANNEL_NAME)='Ancient Aliens' then 'History Brand'
                    when (CHANNEL_NAME)='Mountain Men on The History Channel' then 'History Brand'
                    when (CHANNEL_NAME)='The Best of Pawn Stars' then 'History Brand'
                    when (CHANNEL_NAME)='History Shorts' then 'History Brand'
                    when (CHANNEL_NAME)='Kings of Pain' then 'History Brand'
                    when (CHANNEL_NAME)='The Curse of Oak Island' then 'History Brand'
                    when (CHANNEL_NAME)='Doomsday: 10 Ways The World Will End' then 'History Brand'
                    when (CHANNEL_NAME)='The Strongest Man In History' then 'History Brand'
                    when (CHANNEL_NAME)='Pawn Stars' then 'History Brand'
                    when (CHANNEL_NAME)='History Remade With Sabrina' then 'History Brand'
                    when (CHANNEL_NAME)='American Pickers on  The History Channel' then 'History Brand'
                    when (CHANNEL_NAME)='The Proof is Out There' then 'History Brand'
                    when (CHANNEL_NAME)='When Big Things Go Wrong' then 'History Brand'
                    when (CHANNEL_NAME)='Vikings' then 'History Brand'
                    when (CHANNEL_NAME)='Counting Cars' then 'History Brand'
                    when (CHANNEL_NAME)='Swamp People on History' then 'History Brand'
                    when (CHANNEL_NAME)='Ancient Workouts with Omar' then 'History Brand'
                    when (CHANNEL_NAME)='Forged in Fire' then 'History Brand'
                    when (CHANNEL_NAME)='Ice Road Truckers on HISTORY' then 'History Brand'
                    when (CHANNEL_NAME)='History NOW' then 'History Brand'
                    when (CHANNEL_NAME)='Bring It: More Moves' then 'Lifetime Brand'
                    when (CHANNEL_NAME)='Little Women' then 'Lifetime Brand'
                    when (CHANNEL_NAME)='Dance Moms' then 'Lifetime Brand'
                    when (CHANNEL_NAME)='The Rap Game' then 'Lifetime Brand'
                    when (CHANNEL_NAME)='Cheerleader Generation' then 'Lifetime Brand'
                    when (CHANNEL_NAME)='Bring It' then 'Lifetime Brand'
                    when (CHANNEL_NAME)='Married at First Sight' then 'Lifetime Brand'
                    when (CHANNEL_NAME)='Married At First Sight' then 'Lifetime Brand'
                    when (CHANNEL_NAME)='Lifetime' then 'Lifetime Brand'
                    when (CHANNEL_NAME)='Dance Moms: Full Numbers' then 'Lifetime Brand'
                    when (CHANNEL_NAME)='Fall Into Me' then 'Lifetime Brand'
                    else 'other'
                end as BRAND_ROLL_UP
            FROM
                ({query})
        """

    def map_brand_using_file(self, sql_query: str, brand_table: str) -> str:
        """
        Function:
            * Maps BRAND in table using CHANNEL_NAME.

        Params:
            * sql_query : Table in which BRAND is to be mapped.
            * brand_table : This table/file contains mapping of BRAND and CHANNEL_NAME

        Returns:
            * Output table with BRAND mapped.
        """
        return f"""
            SELECT 
                a.*,
                b.BRAND_ROLL_UP
            FROM ({sql_query}) a
            LEFT JOIN ({brand_table}) b
                ON a.CHANNEL_NAME = b.CHANNEL_NAME
        """

    def map_video_form(self, final_data: str) -> str:
        """
        Function:
            * This maps VIDEO_FORM using VIDEO_TITLE in case statement
        Params:
            * final_data : Input query
        Returns:
            * Mapped VIDEO_FORM query.
        """
        return f"""
            SELECT *,
                CASE
                    WHEN LOWER(UPLOADER_TYPE) = 'self' THEN
                        CASE
                            WHEN 
                                   LOWER(VIDEO_TITLE) LIKE '%mega marathon%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%marathon%' 
                                AND LOWER(VIDEO_TITLE) NOT LIKE '%#shorts%' 
                            THEN 'Marathon'
                            
                            WHEN 
                                   LOWER(VIDEO_TITLE) LIKE '%full episode%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%full episodes%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%full movie%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%full documentary%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%ultimate guide%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%full ep%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%full length%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%husband vanished after 37 years%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%buried: knights templar and the holy grail%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%wanda durant story%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%college admissions scandal%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%james van der beek%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%14-year-old girl goes missing while walking dog%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%he buried teen girl five feet under after night out%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%real estate mom disappears during showing%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%teen cheerleader is abducted by family friend%'
                                AND LOWER(VIDEO_TITLE) NOT LIKE '%part%' 
                                AND LOWER(VIDEO_TITLE) NOT LIKE '%#shorts%' 
                                AND LOWER(VIDEO_TITLE) NOT LIKE '%marathon%' 
                            THEN 'Long Form'
                            
                            WHEN 
                                   LOWER(VIDEO_TITLE) LIKE '%compilation%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%compilations%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%top %' 
                                OR LOWER(VIDEO_TITLE) LIKE '%most viewed moments%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%best moments of all time%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%moments of all time%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%blow your mind%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%jaw-dropping%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%mind-boggling%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%deadliest%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%more super rare%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%horrifying strange creatures%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%rapid fire negotiations%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%most iconic moments%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%greatest hits%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%best of%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%of all time%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%insanely high appraisals%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%rare & patriotic us military items%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%high value appraisals%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%super rare high value items%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%angry sellers lose their cool%'
                                AND DURATION > 780 -- Duration checking in seconds.
                                AND LOWER(VIDEO_TITLE) NOT LIKE '%#shorts%' 
                            THEN 'Compilations'
                            
                            WHEN 
                                   LOWER(VIDEO_TITLE) LIKE '%shorts%' 
                                OR LOWER(VIDEO_TITLE) LIKE '%#shorts%' 
                            THEN 'Vertical Viewing'
                            
                            ELSE 'Short Form'
                        END
                    ELSE ''
                END AS VIDEO_FORM
            FROM ({final_data}) AS subquery
        """

    def check_and_delete_data_old_data(
        self, table_name: str, month: str, is_reprocess_all: bool
    ):
        """
        Function:
            * This will check if processed month data is already present in the output table; if yes, it will delete respective months.
        Params:
            * table_name : Output table path
            * month : Processed months as a comma-separated string
            * is_reprocess_all : If True, it will delete all months data after and including the provided month.
        """
        # Correct DELETE SQL query using IN clause
        if is_reprocess_all == False:
            delete_sql = f"""
            DELETE FROM {table_name}
            WHERE month = ('{month}')
            """
            self.log_message(f"Following Month deleted from table{table_name}: {month}")
        else:
            delete_sql = f"""
            DELETE FROM {table_name}
            WHERE month >= '{month}'
            """
            self.log_message(f"All months data after {month} deleted from {table_name}")

        # Execute the delete statement
        self.session.sql(delete_sql).collect()

    def check_table_exists(self, table_name: str):
        """
        Function:
            * This function checks if the table exists in the database. If not present, it will raise an error.

        Params:
            * table_name : Table name to check in the database.

        Returns:
            * Error message if table not present.
        """
        # Extract database, schema, and table separately
        db, schema, table = table_name.split(".")

        # Snowflake query to check if the table exists using SHOW TABLES
        query = f"""
            SHOW TABLES LIKE '{table}' IN SCHEMA {db}.{schema}
        """
        result = self.session.sql(query).collect()

        if not result:
            raise RuntimeError(
                f"Error: Table '{table_name}' does not exist in the database!"
            )
        else:
            self.log_message(f"Table {table_name} already present in database.")

    def insert_data_into_table(self, table_name: str, data_query: str) -> None:
        """Insert data into a specified Snowflake table."""
        insert_sql = f"INSERT INTO {table_name} {data_query}"
        self.session.sql(insert_sql).collect()

    def send_email_notification(
        self,
        subject: str,
        message: str,
        recipient: str,
        sender: str,
        closing_message: str,
        processing_log_query: str,
    ) -> None:
        """
        Function
            * Sends an email notification via the SQL stored procedure.

        Parameters:
            - subject: The subject of the email.
            - message: The body message of the email.
            - recipient: The recipient's email address.
            - sender: The sender's email address.
            - closing_message: The closing message for the email.
            - processing_log_query: The SQL query that generates the processing log.
            - session: The Snowflake session or your database connection object.
        """

        # Format the SQL statement with the provided parameters
        sql_statement = f"""
        CALL COMMON_DB.ETL.SP_SEND_EMAIL_NOTIFICATION (
                $$ {subject} $$, 
                $$ {message} $$,
                '{recipient}', 
                '{sender}',
                '{closing_message}',
                $$ {processing_log_query} $$
            );
        """

        # Execute the SQL statement using the session
        self.session.sql(sql_statement).collect()
