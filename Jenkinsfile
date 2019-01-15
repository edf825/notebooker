def unit = {
  pyTest 'tests/unit'
}

def integration = {
  pyTest 'tests/integration', "-m 'not compress and not serial'"
}

env.PYTHONPATH = '.:..'  // So that the src directory can be used as a dir

ahlPython {
    buildLabel='ts2-el7'
    labels='ts2-el7'
    buildPinnedEgg = true
    dockerPath = 'docker'
    medusaVersions = ["27-3"]
    publicProject = true

    testStages =  [
        [name: 'Unit',
         body: unit,
        ],
        [name: 'Integration',
         body: integration,
         workers: 1
        ],
    ]
}
